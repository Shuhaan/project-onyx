import pytest
from extract_utils import log_message

class TestLogMessage:
    @pytest.mark.parametrize(
        "level, level_name, message",
        [
            (10, "DEBUG", "This is a debug message"),
            (20, "INFO", "This is an info message"),
            (30, "WARNING", "This is a warning message"),
            (40, "ERROR", "This is an error message"),
            (50, "CRITICAL", "This is a critical message"),
        ],
        ids=[
            "Debug level",
            "Info level",
            "Warning level",
            "Error level",
            "Critical level",
        ],
    )
    def test_log_message_levels(self, caplog, level, level_name, message):
        caplog.set_level(level)
        log_message("function_name", level, message)

        assert caplog.messages == [message]
        assert level_name in caplog.text