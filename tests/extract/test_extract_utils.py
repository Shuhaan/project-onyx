import pytest, logging
from decimal import Decimal
from datetime import datetime
from extract_lambda.utils import get_secret, format_response, log_message


class TestGetSecret:
    def test_get_secret(self, secretsmanager_client):
        secretsmanager_client.create_secret(
            Name="aSecret",
            SecretString=str(
                """{
                        "username":"userId",
                        "password":"password"
                    }"""
            ),
        )
        response = get_secret(secret_name="aSecret", region_name="eu-west-2")
        assert response == {"username": "userId", "password": "password"}


class TestFormatResponse:
    @pytest.mark.parametrize(
        "columns, response, expected",
        [
            (["A", "B"], [[1, 2], [3, 4]], [{"A": 1, "B": 2}, {"A": 3, "B": 4}]),
            (
                ["A", "B"],
                [[1, 2], [Decimal("1.21"), 2]],
                [{"A": 1, "B": 2}, {"A": 1.21, "B": 2}],
            ),
            (
                ["Date", "Value"],
                [[datetime(2024, 8, 18, 12, 0, 0), 1.23]],
                [{"Date": "2024-08-18 12:00:00", "Value": 1.23}],
            ),
        ],
        ids=[
            "Simple integer values",
            "Decimal to float conversion",
            "Datetime formatting",
        ],
    )
    def test_format_response_valid(self, columns, response, expected):
        result = format_response(columns, response)
        assert result == expected

    @pytest.mark.parametrize(
        "columns, response",
        [
            (["A", "B"], [[1, 2], [3]]),
            (["A"], [[1, 2]]),
        ],
        ids=[
            "More columns than values in row",
            "Fewer columns than values in row",
        ],
    )
    def test_format_response_invalid(self, columns, response):
        with pytest.raises(
            ValueError, match="Mismatch between number of columns and row length"
        ):
            format_response(columns, response)


class TestLogMessage:
    def test_log_message(self, caplog):
        caplog.set_level(logging.INFO)
        log_message("function_name", 30, "This is a warning")
        expected = ["This is a warning"]
        assert caplog.messages == expected
        assert "WARNING" in caplog.text
