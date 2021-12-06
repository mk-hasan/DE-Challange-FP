import json
from typing import TypeVar, Callable, Any

T = TypeVar('T')


class DecoratorFactory:
    """
    This is a Decorator class for adding extra behaviour to the methods. Several decorator can be defined here and used later depends on the purpose of the methods.
    """
    F = TypeVar('F', bound=Callable[..., Any])

    @classmethod
    def program_status(self, func: F) -> F:
        """
        Generic decorator fucntion to show when the function will be running
        """

        def new_function(*args):
            print(f"Executing: {func.__name__}")
            output = func(*args)
            print("Finished")
            return output

        return new_function


def color_negative_red(val):
    color = 'red'
    return f'color: {color}'


def load_config():
    """
    This function loads the database configuration and endpoints detials.
    """

    with open('./../config.json', 'r') as j:
        config = json.loads(j.read())
    return config
