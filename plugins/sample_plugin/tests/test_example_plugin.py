import pytest
from dotenv import load_dotenv


load_dotenv()


class TestExamplePlugin:
    def test_regular(self):
        assert True

    # example integration test (run on CI when a release is published)
    @pytest.mark.integration
    async def test_simple(self):
        assert True
