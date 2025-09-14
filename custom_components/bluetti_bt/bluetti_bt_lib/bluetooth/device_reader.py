"""Device reader."""

import asyncio
import logging
from typing import Any, Callable, List, cast
import async_timeout
from bleak import BleakClient, BleakError
from bleak.backends.device import BLEDevice
from bleak_retry_connector import establish_connection

from custom_components.bluetti_bt.bluetti_bt_lib.bluetooth.encryption import BluettiEncryption, Message, MessageType

from ..base_devices.BluettiDevice import BluettiDevice
from ..const import NOTIFY_UUID, RESPONSE_TIMEOUT, WRITE_UUID
from ..exceptions import BadConnectionError, ModbusError, ParseError
from ..utils.commands import ReadHoldingRegisters

_LOGGER = logging.getLogger(__name__)


class DeviceReader:
    def __init__(
        self,
        bleak_client: BleakClient,
        bluetti_device: BluettiDevice,
        future_builder_method: Callable[[], asyncio.Future[Any]],
        persistent_conn: bool = False,
        polling_timeout: int = 45,
        max_retries: int = 5,
        encrypted: bool = False,
        ble_device_callback: Callable[[], BLEDevice] | None = None,
    ) -> None:
        self.client = bleak_client
        self.bluetti_device = bluetti_device
        self.create_future = future_builder_method
        self.persistent_conn = persistent_conn
        self.polling_timeout = polling_timeout
        self.max_retries = max_retries
        self.encrypted = encrypted
        self._ble_device_cb = ble_device_callback

        self.has_notifier = False
        self.notify_future: asyncio.Future[Any] | None = None
        self.current_command = None
        self.notify_response = bytearray()
        # Buffer for accumulating encrypted notifications until a full frame is received
        self._enc_buffer = bytearray()
        self._enc_expected_len: int | None = None

        # polling mutex to guard against switches
        self.polling_lock = asyncio.Lock()

        self.encryption = BluettiEncryption()

    async def read_data(
        self, filter_registers: List[ReadHoldingRegisters] | None = None
    ) -> dict | None:
        _LOGGER.info("Reading data")

        if self.bluetti_device is None:
            _LOGGER.error("Device is None")
            return None

        polling_commands = self.bluetti_device.polling_commands
        pack_commands = self.bluetti_device.pack_polling_commands
        if filter_registers is not None:
            polling_commands = filter_registers
            pack_commands = []
        _LOGGER.info("Polling commands: " + ",".join([f"{c.starting_address}-{c.starting_address + c.quantity - 1}" for c in polling_commands]))
        _LOGGER.info("Pack comands: " + ",".join([f"{c.starting_address}-{c.starting_address + c.quantity - 1}" for c in pack_commands]))

        parsed_data: dict = {}

        async with self.polling_lock:
            try:
                async with async_timeout.timeout(self.polling_timeout):
                    # Reconnect if not connected using bleak_retry_connector
                    if not self.client.is_connected:
                        ble_device = self._ble_device_cb() if self._ble_device_cb else None
                        if ble_device is None:
                            ble_device = self.client.address
                        self.client = await establish_connection(
                            self.client.__class__,
                            ble_device,
                            self.bluetti_device.type,
                            ble_device_callback=self._ble_device_cb,
                            max_attempts=self.max_retries,
                        )

                    # Attach notifier if needed
                    if not self.has_notifier:
                        await self.client.start_notify(
                            NOTIFY_UUID, self._notification_handler
                        )
                        self.has_notifier = True

                    while self.encrypted and not self.encryption.is_ready_for_commands:
                        if not self.client.is_connected:
                            raise BadConnectionError("Disconnected during handshake")
                        await asyncio.sleep(0.5)
                        _LOGGER.debug("Encryption handshake not finished yet")

                    # Execute polling commands
                    for command in polling_commands:
                        try:
                            body = command.parse_response(
                                await self._async_send_command(command)
                            )
                            _LOGGER.debug("Raw data: %s", body)
                            parsed = self.bluetti_device.parse(
                                command.starting_address, body
                            )
                            _LOGGER.debug("Parsed data: %s", parsed)
                            parsed_data.update(parsed)
                        except ParseError:
                            _LOGGER.warning("Got a parse exception")

                    # Execute pack polling commands
                    if len(pack_commands) > 0 and len(self.bluetti_device.pack_num_field) == 1:
                        _LOGGER.debug("Polling battery packs")
                        for pack in range(1, self.bluetti_device.pack_num_max + 1):
                            _LOGGER.debug("Setting pack_num to %i", pack)

                            # Set current pack number
                            command = self.bluetti_device.build_setter_command(
                                "pack_num", pack
                            )
                            body = command.parse_response(
                                await self._async_send_command(command)
                            )
                            _LOGGER.debug("Raw data set: %s", body)

                            # Check set pack_num
                            set_pack = int.from_bytes(body, byteorder='big')
                            if set_pack is not pack:
                                _LOGGER.warning("Pack polling failed (pack_num %i doesn't match expected %i)", set_pack, pack)
                                continue

                            if self.bluetti_device.pack_num_max > 1:
                                # We need to wait after switching packs 
                                # for the data to be available
                                await asyncio.sleep(5)
                            
                            for command in pack_commands:
                                # Request & parse result for each pack
                                try:
                                    body = command.parse_response(
                                        await self._async_send_command(command)
                                    )
                                    parsed = self.bluetti_device.parse(
                                        command.starting_address, body
                                    )
                                    _LOGGER.debug("Parsed data: %s", parsed)

                                    for key, value in parsed.items():
                                        # Ignore likely unavailable pack data
                                        if value != 0:
                                            parsed_data.update({key + str(pack): value})

                                except ParseError:
                                    _LOGGER.warning("Got a parse exception...")

            except TimeoutError as err:
                _LOGGER.error("Polling timed out (%ss). Trying again later", self.polling_timeout)
                return None
            except BleakError as err:
                _LOGGER.error("Bleak error: %s", err)
                return None
            finally:
                # Disconnect if connection not persistant
                if not self.persistent_conn:
                    if self.has_notifier:
                        try:
                            await self.client.stop_notify(NOTIFY_UUID)
                        except:
                            # Ignore errors here
                            pass
                        self.has_notifier = False
                    await self.client.disconnect()
                # Always clear any partial encrypted buffers between polling cycles
                self._enc_buffer.clear()
                self._enc_expected_len = None

            # Check if dict is empty
            if not parsed_data:
                return None

            # Reset Encryption keys
            self.encryption.reset()

            return parsed_data

    async def _async_send_command(self, command: ReadHoldingRegisters) -> bytes:
        """Send command and return response"""
        try:
            # Prepare to make request
            self.current_command = command
            self.notify_future = self.create_future()
            self.notify_response = bytearray()

            # Make request
            _LOGGER.debug("Requesting %s", command)

            command_bytes = bytes(command)

            # Encrypt command
            if self.encrypted is True:
                if not self.encryption.is_ready_for_commands:
                    return bytes()
                command_bytes = self.encryption.aes_encrypt(command_bytes, self.encryption.secure_aes_key, None)

            await self.client.write_gatt_char(WRITE_UUID, command_bytes)

            # Wait for response
            res = await asyncio.wait_for(self.notify_future, timeout=RESPONSE_TIMEOUT)

            # Process data
            _LOGGER.debug("Got %s bytes", len(res))
            return cast(bytes, res)

        except TimeoutError:
            _LOGGER.debug("Polling single command timed out")
        except ModbusError as err:
            _LOGGER.debug(
                "Got an invalid request error for %s: %s",
                command,
                err,
            )
        except (BadConnectionError, BleakError) as err:
            # Ignore other errors
            pass

        # caught an exception, return empty bytes object
        return bytes()

    async def _notification_handler(self, _sender: int, data: bytearray):
        """Handle bt data."""

        # Handle encrypted data
        if self.encrypted is True:
            # First check if this is a pre-key-exchange control message (starts with magic "**")
            try:
                message = Message(data)
            except Exception:
                message = None  # Will be treated as encrypted payload below

            if message is not None and message.is_pre_key_exchange:
                try:
                    message.verify_checksum()

                    if message.type == MessageType.CHALLENGE:
                        challenge_response = self.encryption.msg_challenge(message)
                        if challenge_response is not None:
                            await self.client.write_gatt_char(WRITE_UUID, challenge_response)
                        return

                    if message.type == MessageType.CHALLENGE_ACCEPTED:
                        _LOGGER.debug("Challenge accepted")
                        return
                except Exception as err:
                    _LOGGER.warning("Error handling pre-key message: %s", err)
                    return

            # From here on, treat data as part of one or more encrypted frames.
            # Accumulate, then parse and process as many complete frames as present.
            self._enc_buffer.extend(data)

            while True:
                # Determine expected encrypted frame length once we have enough header bytes
                if self._enc_expected_len is None:
                    # We need at least 2 bytes for plaintext length. In secure phase we also need 4 bytes IV seed.
                    need = 6 if self.encryption.secure_aes_key is not None else 2
                    if len(self._enc_buffer) < need:
                        return

                    # Parse the plaintext length from the first 2 bytes
                    plain_len = (self._enc_buffer[0] << 8) + self._enc_buffer[1]
                    padding = (16 - (plain_len % 16)) % 16
                    iv_overhead = 4 if self.encryption.secure_aes_key is not None else 0
                    self._enc_expected_len = 2 + iv_overhead + plain_len + padding

                if len(self._enc_buffer) < self._enc_expected_len:
                    # Not enough bytes yet for a full frame
                    return

                # We have at least one complete frame; if more bytes exist, keep them for the next loop
                enc_frame = bytes(self._enc_buffer[: self._enc_expected_len])
                # Trim used bytes from buffer and reset expected len to recalc for any following frame
                del self._enc_buffer[: self._enc_expected_len]
                self._enc_expected_len = None

                try:
                    key, iv = self.encryption.getKeyIv()
                    decrypted_bytes = self.encryption.aes_decrypt(enc_frame, key, iv)
                except ValueError as err:
                    # Commonly happens when receiving a partial or stray notification; ignore gracefully
                    _LOGGER.debug("Failed to decrypt frame (%s). Ignoring.", err)
                    continue
                except Exception as err:
                    _LOGGER.warning("Decrypt error: %s", err)
                    continue

                # Decrypted bytes may either be another key-exchange message or a regular payload
                try:
                    decrypted = Message(decrypted_bytes)
                except Exception:
                    decrypted = None

                if decrypted is not None and decrypted.is_pre_key_exchange:
                    try:
                        decrypted.verify_checksum()

                        if decrypted.type == MessageType.PEER_PUBKEY:
                            try:
                                peer_pubkey_response = self.encryption.msg_peer_pubkey(decrypted)
                            except Exception as err:
                                _LOGGER.warning("Peer pubkey handling failed: %s", err)
                                # Reset encryption to force a fresh handshake
                                self.encryption.reset()
                                # Clear any buffered frames since keys changed/reset
                                self._enc_buffer.clear()
                                self._enc_expected_len = None
                                try:
                                    await self.client.disconnect()
                                except Exception:
                                    pass
                                self.has_notifier = False
                                continue
                            await self.client.write_gatt_char(WRITE_UUID, peer_pubkey_response)
                            continue

                        if decrypted.type == MessageType.PUBKEY_ACCEPTED:
                            try:
                                self.encryption.msg_key_accepted(decrypted)
                            except Exception as err:
                                _LOGGER.warning("Key acceptance failed: %s", err)
                                self.encryption.reset()
                                # Clear any buffered frames since keys changed/reset
                                self._enc_buffer.clear()
                                self._enc_expected_len = None
                                try:
                                    await self.client.disconnect()
                                except Exception:
                                    pass
                                self.has_notifier = False
                                continue
                    except Exception as err:
                        _LOGGER.warning("Error handling decrypted pre-key message: %s", err)
                        try:
                            await self.client.disconnect()
                        except Exception:
                            pass
                        self.has_notifier = False
                        continue

                # Treat decrypted bytes as the actual payload and feed into plain handler logic
                await self._handle_plain_data(decrypted_bytes)

            # Nothing else to do here; the plain handler above dealt with the data
            return

        # Non-encrypted path: handle as plain data
        await self._handle_plain_data(data)

    async def _handle_plain_data(self, data: bytes | bytearray):
        """Handle plaintext payload bytes by accumulating and fulfilling the notify future."""
        # Ignore notifications we don't expect
        if self.notify_future is None or self.notify_future.done():
            # Some devices emit unsolicited notifications between requests.
            # These are harmless; log at debug to avoid noisy warnings.
            _LOGGER.debug("Unexpected notification (ignored), bytes=%d", len(data) if data is not None else 0)
            return

        # If something went wrong, we might get weird data.
        if data == b"AT+NAME?\r" or data == b"AT+ADV?\r":
            err = BadConnectionError("Got AT+ notification")
            self.notify_future.set_exception(err)
            return

        # Save data
        self.notify_response.extend(data)

        # Check for completion or exception
        if len(self.notify_response) == self.current_command.response_size():
            if self.current_command.is_valid_response(self.notify_response):
                self.notify_future.set_result(self.notify_response)
            else:
                self.notify_future.set_exception(ParseError("Failed checksum"))
        elif self.current_command.is_exception_response(self.notify_response):
            # We got a MODBUS command exception
            msg = f"MODBUS Exception {self.current_command}: {self.notify_response[2]}"
            self.notify_future.set_exception(ModbusError(msg))
