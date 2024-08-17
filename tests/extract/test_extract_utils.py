import pytest, logging
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
        [(["A", "B"], [[1, 2], [10, 11]], [{"A": 1, "B": 2}, {"A": 10, "B": 11}])],
    )
    def test_format_response(self, columns, response, expected):
        result = format_response(columns, response)
        assert result == expected


class TestLogMessage:
    def test_log_message(self, caplog):
        caplog.set_level(logging.INFO)
        log_message("function_name", 30, "This is a warning")
        expected = ["This is a warning"]
        assert caplog.messages == expected
        assert "WARNING" in caplog.text
