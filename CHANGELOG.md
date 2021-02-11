# Changelog

## v0.7.2 2021-02-11

Fix an aio-pika deprecation, to use async context managers.

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
