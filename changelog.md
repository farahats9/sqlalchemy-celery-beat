# Change history

## v0.5.2

- Add Validations to all models
- Use `Enum` instead of free text for `IntervalSchedule.period` and `SolarSchedule.event`
- Use generic foreign key for schedules in `PeriodicTask`
- Make sessions efficient and only use a session when needed
- Allow using a separate schema for the tables
- Add `ClockedSchedule`

## v0.3.0

- Support Celery 5.0.1
