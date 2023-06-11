from nodalize.data_management.kdb_data_manager import KdbDataManager


class BaseTestKdbDataManager:
    NAMESPACE = "test"

    @staticmethod
    def get_credentials():
        return None, None

    @staticmethod
    def create_kdb_data_manager(namespace=NAMESPACE, host="localhost", port=5000):
        return KdbDataManager(
            namespace, host, port, BaseTestKdbDataManager.get_credentials
        )

    @staticmethod
    def clean_up():
        try:
            kdb_manager = BaseTestKdbDataManager.create_kdb_data_manager(
                BaseTestKdbDataManager.NAMESPACE
            )
        except Exception:
            return

        try:
            kdb_manager.execute_query("delete from `.", read_only=False)
        except Exception:
            pass

        try:
            kdb_manager.delete_from_kdb(None)
        except Exception:
            pass
