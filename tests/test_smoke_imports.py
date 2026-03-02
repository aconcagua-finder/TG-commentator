import unittest


class TestSmokeImports(unittest.TestCase):
    def test_import_admin_web(self) -> None:
        import admin_web.main  # noqa: F401

    def test_import_commentator(self) -> None:
        # Import should not trigger network calls (script entry point is guarded by __main__).
        import commentator  # noqa: F401
