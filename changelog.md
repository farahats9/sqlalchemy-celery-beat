# Change history

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
