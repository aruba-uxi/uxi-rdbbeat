[![Python Version](https://img.shields.io/badge/python-3.8-blue?logo=Python&logoColor=yellow)](https://docs.python.org/3.8/)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![Build: just](https://img.shields.io/badge/%F0%9F%A4%96%20build-just-black?labelColor=white)](https://just.systems/)
[![code](https://github.com/aruba-uxi/celery-sqlalchemy-scheduler/actions/workflows/lint-test-code.yaml/badge.svg)](https://github.com/aruba-uxi/celery-sqlalchemy-scheduler/actions/workflows/lint-test-code.yaml)

# UXI Celery Scheduler

A SQLAlchemy-based scheduler for celery-beat.

## Table Of Contents

- [Installation](#installation)
- [Celery Configuration](#celery-configuration)
- [Usage](#usage)
- [Deployment](#deployment)
- [Contribution](#contribution)
    - [Setup](#setup)
    - [Version Control](#version-control)
    - [Workflows](#workflows)
- [License](#license)
- [Acknowledgements](#acknowledgements)


## Installation

The library is available for download from the official pypi website:

`pip install celery-rdbbeat`

Or you can build the library for the source code hosted publicly on GitHub.

## Celery Configuration

The library makes use of the parent service's database and scope management mechanism.
You can configure SQLAlchemy `session_scope` when you configure celery, for example as:

```Python
from celery import Celery

# this comes from parent service
from server.db import session_scope

celery = Celery('tasks')

celery.conf.update(
    {'session_scope': session_scope}
)
```

## Usage
### Creating crontab-based periodic task

A crontab schedule has the fields: `minute`, `hour`, `day_of_week`,
`day_of_month` and `month_of_year`, so if you want the equivalent of a
`30 * * * *` (execute every 30 minutes) crontab entry, you specify:

```Python
from uxi_celery_scheduler.controller import schedule_task
from uxi_celery_scheduler.data_models import ScheduledTask

scheduled_task = {
    "name": "task_1",
    "task": "echo",
    "schedule": {
        "minute": "30",
        "hour": "*",
        "day_of_week": "*",
        "day_of_month": "*",
        "month_of_year": "*",
        "timezone": "UTC",
    },
}

with session_scope() as session:
    task = schedule_task(session, ScheduledTask.parse_ob(scheduled_task))
```

The crontab schedule is linked to a specific timezone using the
`timezone` input parameter.


## Deployment

The periodic tasks still need 'workers' to execute them. So make sure
the default **Celery** package is installed. (If not installed, please
follow the installation instructions here:
<https://github.com/celery/celery>)

Both the worker and beat services need to be running at the same time.

1.  Start a Celery worker service (specify your project name):

        $ celery -A [project-name] worker --loglevel=info

2.  As a separate process, start the beat service (specify the
    scheduler):

        $ celery -A [project-name] beat -l info --scheduler uxi_celery_scheduler.schedulers:DatabaseScheduler


# Contribution
## Setup



This project is setup to use [editorconfig](https://editorconfig.org/). Most editors work but some require a plugin like [VSCode](https://marketplace.visualstudio.com/items?itemName=EditorConfig.EditorConfig)

It's advisable to create a virtual environment for this project to keep packages separate.
> **__NOTE__:** Using pyenv, you can run `pyenv virtualenv 3.10.<latest> celery-sqlalchemy-scheduler`

After creating a virtual environment, install the required dependencies.

```sh
just setup-dev
```

## Version Control

This repo follows the [SemVer 2](https://semver.org/) version format.

Given a version number `MAJOR.MINOR.PATCH`, increment the:

- `MAJOR` version when you make incompatible API changes,
- `MINOR` version when you add functionality in a backwards compatible manner, and
- `PATCH` version when you make backwards compatible bug fixes.

## Workflows

The repository has a number of github workflows defined in the the `.github/workflows` folder.



### Lint & Test Code

- Tests the code for linting issues
- Tests the requirements file for any changes
- Runs the provided unit tests to ensure code quality

### Release

- Pushes the library to Pypi


## License
MIT licensed. See the bundled LICENSE file for more details.

## Acknowledgements

- [django-celery-beat](https://github.com/celery/django-celery-beat)
- [celerybeatredis](https://github.com/liuliqiang/celerybeatredis)
- [celery](https://github.com/celery/celery)
- [celery-sqlalchemy-scheduler](https://github.com/AngelLiang/celery-sqlalchemy-scheduler)
