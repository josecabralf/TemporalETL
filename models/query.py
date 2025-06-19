from dataclasses import dataclass


@dataclass
class IQuery:
    """
    Interface for query operations.
    """
    @staticmethod
    def from_dict(data: dict):
        """
        Create an instance from a dictionary.
        :param data: Dictionary containing query parameters.
        :return: An instance of the query.
        """
        raise NotImplementedError("Subclasses must implement this method.")

    def to_summary_base(self) -> dict:
        """
        Convert the query to a dictionary format.
        :return: Dictionary representation of the query.
        """
        raise NotImplementedError("Subclasses must implement this method.")


class QueryFactory:
    """
    Factory class to create query instances based on type.
    """

    # Add other query types here as needed
    queryTypes = {
        # "IQuery": "models.query.IQuery",
        "LaunchpadQuery": "launchpad.query.LaunchpadQuery",
    }

    @staticmethod
    def create(query_type: str, args: dict) -> IQuery:
        """
        Create a query instance based on the type.
        :param query_type: The type of query to create.
        :param kwargs: Additional parameters for the query.
        :return: An instance of the specified query type.
        """
        print(query_type)
        if query_type not in QueryFactory.queryTypes:
            raise ValueError(f"Unknown query type: %s", query_type)

        module_name, class_name = QueryFactory.queryTypes[query_type].rsplit('.', 1)
        module = __import__(module_name, fromlist=[class_name])
        query_class = getattr(module, class_name)

        if not issubclass(query_class, IQuery):
            raise TypeError(f"{query_class.__name__} is not a subclass of IQuery")

        return query_class.from_dict(args)