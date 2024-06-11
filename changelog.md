# Change history

## v0.8.0

- improve examples
- allow passing options to sqlalchemy engine (fix #9)
- fix issue in deletion (fix #10)
- fix timezone sometimes not apply correctly
- fix default field values not correctly populating on init object
- fix crontab sometimes changing 0 into *

## v0.7.1

- fix a bug where schedules with the same ID and different descriminators were selected when loading the `PeriodicTask` thanks to [Matthew Bronstein](https://github.com/mbronstein1) and [Bazyl Horsey](https://github.com/bazylhorsey)

## v0.7.0

- added tests
- fixed bug in `IntervalSchedule` repr function
- fixed bug in `PeriodicTaskChanged` insertion

## v0.6.2

- fixed bug where `one_off` is defaulting to `True`

## v0.6.1

- Add Validations to all models
- Use `Enum` instead of free text for `IntervalSchedule.period` and `SolarSchedule.event`
- Use generic foreign key for schedules in `PeriodicTask`
- Make sessions efficient and only use a session when needed
- Allow using a separate schema for the tables
- Add `ClockedSchedule`
- Improve timezone support

## v0.3.0

- Support Celery 5.0.1
