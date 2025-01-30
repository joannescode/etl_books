import logging
import logging.handlers


def get_logger():
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
