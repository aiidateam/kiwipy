# Changelog

## `v0.8.3` - 2022-11-21

### Dependencies

- Add support for Python 3.11 [[#129]](https://github.com/aiidateam/kiwipy/pull/129)
- Update requirement `pyyaml~=6.0` [[#128]](https://github.com/aiidateam/kiwipy/pull/128)


## `v0.8.2` - 2022-10-28

### Dependencies

- Remove `aio-pika` and `pytray` from base requirements [[#126]](https://github.com/aiidateam/kiwipy/pull/126)
- Remove the `nest-asyncio` requirement [[#126]](https://github.com/aiidateam/kiwipy/pull/126)


## `v0.8.1` - 2022-10-20

### Fixes

- Restore intended interface of `RmqThreadCommunicator` [[#124]](https://github.com/aiidateam/kiwipy/pull/124)

### Deprecations

- `RmqThreadCommunicator`: remove deprecated `start` and `stop` methods [[#125]](https://github.com/aiidateam/kiwipy/pull/125)

### Dependencies

- Move `shortuuid` from `test` extra to main [[#123]](https://github.com/aiidateam/kiwipy/pull/123)
- Unpin `docutils` from the `docs` extra [[#123]](https://github.com/aiidateam/kiwipy/pull/123)


## `v0.8.0` - 2022-10-13

### Dependencies

- Update requirement for `aio-pika~=8.2` [[#114]](https://github.com/aiidateam/kiwipy/pull/114)
- Add support for Python 3.10 [[#120]](https://github.com/aiidateam/kiwipy/pull/120)

### Devops

- Remove obsolete `release.sh` [[#115]](https://github.com/aiidateam/kiwipy/pull/115)
- Merge separate license files into one [[#116]](https://github.com/aiidateam/kiwipy/pull/116)
- Add the `isort` pre-commit hook [[#118]](https://github.com/aiidateam/kiwipy/pull/118)
- Move package into the `src/` subdirectory [[#119]](https://github.com/aiidateam/kiwipy/pull/119)
- Update the continuous deployment workflow [[#121]](https://github.com/aiidateam/kiwipy/pull/121)


## `v0.7.6` - 2022-08-05

- Dependencies: restrict ranges of `aio-pika<6.8.2` and `pamqp~=2.0` [[#108]](https://github.com/aiidateam/kiwipy/pull/110)

## `v0.7.5` - 2022-01-17

- Drop support for Python 3.6 [[#108]](https://github.com/aiidateam/kiwipy/pull/108)
- `RmqCommunicator`: add the `server_properties` property [[#107]](https://github.com/aiidateam/kiwipy/pull/107)
- Expose `aio_pika.Connection.add_close_callback` [[#104]](https://github.com/aiidateam/kiwipy/pull/104)

## `v0.7.4` - 2021-03-02

- ♻️ REFACTOR: BroadcastFilter to extract filter conditions into a separate `is_filtered` method.

## `v0.7.3` - 2021-02-24

- 👌 IMPROVE: Add debug logging for sending task/rpc/broadcast to RMQ.
- 👌 IMPROVE: Close created asyncio loop on RmqThreadCommunicator.close

## `v0.7.2` - 2021-02-11

- 🐛 FIX: an aio-pika deprecation, to use async context managers when processing messages.

## `v0.7.1` - 2020-12-09

The default task message TTL setting was changed in `v0.5.4` but this breaks existing queues since RabbitMQ does not allow changing these parameters on existing queues.
Therefore the change was reverted which was released in `v0.5.5`.
However, since that was a patch release, it had not been merged back to `v0.6.0` as well, which therefore from the problem described.
The same revert is applied in this release to restore original functionality.

### Changes
- Revert "Increase the default TTL for task messages" [[#93]](https://github.com/aiidateam/kiwipy/pull/93)


## `v0.7.0` - 2020-11-04

### Changes
- Add support for Python 3.9 [[#87]](https://github.com/aiidateam/kiwipy/pull/87)
- Drop support for Python 3.5 [[#89]](https://github.com/aiidateam/kiwipy/pull/89)
- Replace old format string interpolation with f-strings [[#90]](https://github.com/aiidateam/kiwipy/pull/90)

### Bug fixes
- Fix warning caused by excepted task and no reply [[#83]](https://github.com/aiidateam/kiwipy/pull/83)

### Dependencies
- Dependencies: update upper limit requirement for `pytray>=0.2.2,<0.4.0` [[#80]](https://github.com/aiidateam/kiwipy/pull/80)
- Dependencies: update requirement `pytest-asyncio~=0.12` [[#82]](https://github.com/aiidateam/kiwipy/pull/82)
