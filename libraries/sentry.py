"""Configure sentry"""

from functools import wraps
import os
import sentry_sdk


def sentry_setup(func):
    """Decorator, to configure sentry"""

    @wraps(func)
    def sentry_setup_wrap(*args, **kwargs):
        sentry_sdk.init(
            dsn=os.environ["ENV_DSN_SENTRY"],
            environment=os.environ["ENVIRONMENT"],
            traces_sample_rate=1.0,
            profiles_sample_rate=1.0
        )        
        result = func(*args, **kwargs)

        return result

    return sentry_setup_wrap
