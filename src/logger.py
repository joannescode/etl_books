import logging
import logging.handlers


def get_logger():
    """Function for created logger object and register all actions in ETL process

    Returns:
        logger object: return logger where you use in all functions for register process
    """
    register = logging.getLogger(__name__)
    if not register.hasHandlers():
        register.setLevel(logging.INFO)

        formatter = logging.Formatter(
            "%(asctime)s - %(name)s:%(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )

        file_handler = logging.FileHandler(
            filename="registers.log", mode="a", encoding="utf-8"
        )
        file_handler.setFormatter(formatter)
        register.addHandler(file_handler)

        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        register.addHandler(console_handler)

    return register
