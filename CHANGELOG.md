# Changelog

## v0.7.8 2023-11-13

- Dependencies: Add support for Python 3.12 [[7aef66f]](https://github.com/aiidateam/kiwipy/commit/7aef66f69e34386aff78a9b9ac988fc377ca49e5)
- Dependencies: Drop support Python 3.7 [[08e77e9]](https://github.com/aiidateam/kiwipy/commit/08e77e96e48de92391cea3c4c0ef7fadb0e0a23e)
- Dependencies: Update `pylint==2.17.7` [[5fafe43]](https://github.com/aiidateam/kiwipy/commit/5fafe43c8dc4c5a6f6a3d2ffb6383db2fd97a246)
- Devops: Fix the ReadTheDocs build [[59cd6f4]](https://github.com/aiidateam/kiwipy/commit/59cd6f40974e764db03bf127f34adc9f5ec1a4a4)

## v0.7.7 2022-08-05

- Dependencies: Add support for Python 3.10 and 3.11 [[#130]](https://github.com/aiidateam/kiwipy/pull/130)
- Dependencies: Update requirement `pytest-notebook>=0.8.1` [[#130]](https://github.com/aiidateam/kiwipy/pull/130)
- Dependencies: Unpin requirement `docutils` [[#130]](https://github.com/aiidateam/kiwipy/pull/130)
- Dependencies: Update requirement `pytest~=6.0` [[#130]](https://github.com/aiidateam/kiwipy/pull/130)
- Dependencies: Update requirement `pyyaml~=6.0` [[#130]](https://github.com/aiidateam/kiwipy/pull/130)

## v0.7.6 2022-08-05

- Dependencies: restrict ranges of `aio-pika<6.8.2` and `pamqp~=2.0` [[#110]](https://github.com/aiidateam/kiwipy/pull/110)

## v0.7.5 2022-01-17

- Drop support for Python 3.6 [[#108]](https://github.com/aiidateam/kiwipy/pull/108)
- `RmqCommunicator`: add the `server_properties` property [[#107]](https://github.com/aiidateam/kiwipy/pull/107)
- Expose `aio_pika.Connection.add_close_callback` [[#104]](https://github.com/aiidateam/kiwipy/pull/104)

## v0.7.4 2021-03-02

- ♻️ REFACTOR: BroadcastFilter to extract filter conditions into a separate `is_filtered` method.

## v0.7.3 2021-02-24

- 👌 IMPROVE: Add debug logging for sending task/rpc/broadcast to RMQ.
- 👌 IMPROVE: Close created asyncio loop on RmqThreadCommunicator.close

## v0.7.2 2021-02-11

- 🐛 FIX: an aio-pika deprecation, to use async context managers when processing messages.

## v0.7.1

The default task message TTL setting was changed in `v0.5.4` but this breaks existing queues since RabbitMQ does not allow changing these parameters on existing queues.
Therefore the change was reverted which was released in `v0.5.5`.
However, since that was a patch release, it had not been merged back to `v0.6.0` as well, which therefore from the problem described.
The same revert is applied in this release to restore original functionality.

### Changes
- Revert "Increase the default TTL for task messages" [[#93]](https://github.com/aiidateam/kiwipy/pull/93)


## v0.7.0

### Changes
- Add support for Python 3.9 [[#87]](https://github.com/aiidateam/kiwipy/pull/87)
- Drop support for Python 3.5 [[#89]](https://github.com/aiidateam/kiwipy/pull/89)
- Replace old format string interpolation with f-strings [[#90]](https://github.com/aiidateam/kiwipy/pull/90)

### Bug fixes
- Fix warning caused by excepted task and no reply [[#83]](https://github.com/aiidateam/kiwipy/pull/83)

### Dependencies
- Dependencies: update upper limit requirement for `pytray>=0.2.2,<0.4.0` [[#80]](https://github.com/aiidateam/kiwipy/pull/80)
- Dependencies: update requirement `pytest-asyncio~=0.12` [[#82]](https://github.com/aiidateam/kiwipy/pull/82)
