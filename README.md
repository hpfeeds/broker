# hpfeeds-broker

[![PyPI](https://img.shields.io/pypi/v/hpfeeds-broker.svg)](https://pypi.python.org/pypi/hpfeeds-broker)
[![Codecov](https://img.shields.io/codecov/c/github/hpfeeds/broker.svg)](https://codecov.io/gh/hpfeeds/broker)
[![Read the Docs](https://readthedocs.org/projects/hpfeeds/badge/?version=latest)](https://hpfeeds.readthedocs.io/en/latest/?badge=latest)

## About

hpfeeds-broker is a simple python 3 asyncio implementation of a hpfeeds broker.

## Installation

You can install it with with `pip`.

```bash
pip install hpfeeds-broker
```

You can also run a broker with Docker:

```
docker run -p "0.0.0.0:20000:20000" -p "0.0.0.0:9431:9431" hpfeeds/hpfeeds-broker:latest
```

It will store access keys in an sqlite database in `/app/var`. The `sqlite` client installed in the container for managing access. You should make sure `/app/var` is a volume. Your clients can connect to port `20000`, and prometheus can connect on port `9431`.

More detailed documentation is available at https://broker.hpfeeds.org.
