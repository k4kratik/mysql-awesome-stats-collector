"""MySQL Awesome Stats Collector (MASC) - DevOps Diagnostic Tool"""


def get_version() -> str:
    """Get the application version.
    
    Returns the installed package version if running as a pip package,
    otherwise returns 'development' for local development.
    """
    try:
        from importlib.metadata import version, PackageNotFoundError
        try:
            return version("mysql-awesome-stats-collector")
        except PackageNotFoundError:
            return "development"
    except ImportError:
        # Python < 3.8 fallback
        try:
            import pkg_resources
            return pkg_resources.get_distribution("mysql-awesome-stats-collector").version
        except pkg_resources.DistributionNotFound:
            return "development"


__version__ = get_version()
