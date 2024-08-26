from dataclasses import dataclass, field
from typing import Any, Iterable, Optional, List
from snakemake_interface_storage_plugins.settings import StorageProviderSettingsBase
from snakemake_interface_storage_plugins.storage_provider import (  # noqa: F401
    StorageProviderBase,
    StorageQueryValidationResult,
    ExampleQuery,
    Operation,
    QueryType,
)
from snakemake_interface_storage_plugins.storage_object import (
    StorageObjectRead,
    StorageObjectWrite,
    StorageObjectGlob,
    retry_decorator,
)
from snakemake_interface_storage_plugins.io import IOCacheStorageInterface

from DIRAC import initialize
from DIRAC.Interfaces.API.Dirac import Dirac
from DIRAC.Core.Utilities.ReturnValues import returnValueOrRaise
from DIRAC.FrameworkSystem.private.standardLogging.LoggingRoot import LoggingRoot

# Optional:
# Define settings for your storage plugin (e.g. host url, credentials).
# They will occur in the Snakemake CLI as --storage-<storage-plugin-name>-<param-name>
# Make sure that all defined fields are 'Optional' and specify a default value
# of None or anything else that makes sense in your case.
# Note that we allow storage plugin settings to be tagged by the user. That means,
# that each of them can be specified multiple times (an implicit nargs=+), and
# the user can add a tag in front of each value (e.g. tagname1:value1 tagname2:value2).
# This way, a storage plugin can be used multiple times within a workflow with different
# settings.
@dataclass
class StorageProviderSettings(StorageProviderSettingsBase):
    storage_element: Optional[str] = field(
        default=None,
        metadata={
            "help": "The DIRAC storage element to uoload the files to.",
            "env_var": False,
            "required": True,
        },
    )


# Required:
# Implementation of your storage provider
# This class can be empty as the one below.
# You can however use it to store global information or maintain e.g. a connection
# pool.
class StorageProvider(StorageProviderBase):
    # For compatibility with future changes, you should not overwrite the __init__
    # method. Instead, use __post_init__ to set additional attributes and initialize
    # futher stuff.

    def __post_init__(self):
        # This is optional and can be removed if not needed.
        # Alternatively, you can e.g. prepare a connection to your storage backend here.
        # and set additional attributes.

        # Set the log level
        dirac_logger = LoggingRoot()
        dirac_logger.setLevel("FATAL")
        #dirac_logger.disableLogsFromExternalLibs()

        # Initialize DIRAC
        initialize()

        # Create a DIRAC instance
        self.dirac = Dirac()

    @classmethod
    def example_queries(cls) -> List[ExampleQuery]:
        """Return an example queries with description for this storage provider (at
        least one)."""
        return [
            ExampleQuery(
                query="LFN:/organisation/user/s/someuser/somefile.txt",
                description="Example query for a Logical File Name (LFN).",
                query_type=QueryType.ANY,
            )
        ]

    def rate_limiter_key(self, query: str, operation: Operation) -> Any:
        """Return a key for identifying a rate limiter given a query and an operation.

        This is used to identify a rate limiter for the query.
        E.g. for a storage provider like http that would be the host name.
        For s3 it might be just the endpoint URL.
        """
        ...

    def default_max_requests_per_second(self) -> float:
        """Return the default maximum number of requests per second for this storage
        provider."""
        ...

    def use_rate_limiter(self) -> bool:
        """Return False if no rate limiting is needed for this provider."""
        False

    @classmethod
    def is_valid_query(cls, query: str) -> StorageQueryValidationResult:
        """Return whether the given query is valid for this storage provider."""
        # Ensure that also queries containing wildcards (e.g. {sample}) are accepted
        # and considered valid. The wildcards will be resolved before the storage
        # object is actually used.
        
        # TODO: Implement a more sophisticated validation
        if query.startswith("LFN:"):
            return StorageQueryValidationResult(
                query=query,
                valid=True)
        else:
            return StorageQueryValidationResult(
                query=query,
                valid=False, 
                reason="Query must start with 'LFN:'")


# Required:
# Implementation of storage object. If certain methods cannot be supported by your
# storage (e.g. because it is read-only see
# snakemake-storage-http for comparison), remove the corresponding base classes
# from the list of inherited items.
class StorageObject(StorageObjectRead, StorageObjectWrite, StorageObjectGlob):
    # For compatibility with future changes, you should not overwrite the __init__
    # method. Instead, use __post_init__ to set additional attributes and initialize
    # futher stuff.

    def __post_init__(self):
        # This is optional and can be removed if not needed.
        # Alternatively, you can e.g. prepare a connection to your storage backend here.
        # and set additional attributes.
        pass

    def retrieve_catalog_directory(self):
        """Retrieve the catalog directory for the current directory of self.query()."""

        # Get the catalog directory
        self.dirname, self.filename = self.query.rsplit("/", 1)
        self.fullname = self.query.removeprefix("LFN:")
        self.CatalogDirectory = returnValueOrRaise(self.provider.dirac.listCatalogDirectory(self.dirname, printOutput=False))

        # check for empty Failed dict (maybe unnecessary because of exists() method)
        if self.CatalogDirectory["Failed"]:
            raise FileNotFoundError(f"Directory {self.dirname} does not exist")


    async def inventory(self, cache: IOCacheStorageInterface):
        """From this file, try to find as much existence and modification date
        information as possible. Only retrieve that information that comes for free
        given the current object.
        """
        # This is optional and can be left as is

        # If this is implemented in a storage object, results have to be stored in
        # the given IOCache object, using self.cache_key() as key.
        # Optionally, this can take a custom local suffix, needed e.g. when you want
        # to cache more items than the current query: self.cache_key(local_suffix=...)
        pass

    def get_inventory_parent(self) -> Optional[str]:
        """Return the parent directory of this object."""
        # this is optional and can be left as is
        return None

    def local_suffix(self) -> str:
        """Return a unique suffix for the local path, determined from self.query."""
        # Warning: DIRAC only keeps the file name and removes the rest of the path
        self.fullname = self.query.removeprefix("LFN:")
        if self.fullname.startswith("/"):
            loc_suffix = self.fullname.removeprefix("/")
        else:
            loc_suffix = self.fullname
        return loc_suffix

    def cleanup(self):
        """Perform local cleanup of any remainders of the storage object."""
        # self.local_path() should not be removed, as this is taken care of by
        # Snakemake.
        ...

    # Fallible methods should implement some retry logic.
    # The easiest way to do this (but not the only one) is to use the retry_decorator
    # provided by snakemake-interface-storage-plugins.
    @retry_decorator
    def exists(self) -> bool:
        # return True if the object exists
        self.retrieve_catalog_directory()
        status_exists = False
        for key, value in self.CatalogDirectory["Successful"].items():
            if self.fullname in value["Files"].keys():
                status_exists = True

        return status_exists

    @retry_decorator
    def mtime(self) -> float:
        # return the modification time
        self.retrieve_catalog_directory()
        ModDate = self.CatalogDirectory["Successful"][self.dirname]["Files"][self.fullname]["MetaData"]["ModificationDate"]
        return ModDate.timestamp()

    @retry_decorator
    def size(self) -> int:
        # return the size in bytes
        self.retrieve_catalog_directory()
        return self.CatalogDirectory["Successful"][self.dirname]["Files"][self.fullname]["MetaData"]["Size"]

    @retry_decorator
    def retrieve_object(self):
        # Ensure that the object is accessible locally under self.local_path()
        destDir = self.local_path().parent
        getFile = returnValueOrRaise(self.provider.dirac.getFile(self.query, destDir=destDir, printOutput=False))

        if getFile["Failed"]:
            raise FileNotFoundError(f"File {self.query} could not be retrieved")

    # The following to methods are only required if the class inherits from
    # StorageObjectReadWrite.

    @retry_decorator
    def store_object(self):
        # Ensure that the object is stored at the location specified by
        # self.local_path().
        addFile = returnValueOrRaise(self.provider.dirac.addFile(self.query, str(self.local_path()), self.provider.settings.storage_element, printOutput=False))

        if addFile["Failed"]:
            raise FileNotFoundError(f"File {self.local_path()} could not be stored to {self.query}")

    @retry_decorator
    def remove(self):
        # Remove the object from the storage.
        removeFile = returnValueOrRaise(self.provider.dirac.removeFile(self.query, printOutput=False))

        if removeFile["Failed"]:
            raise FileNotFoundError(f"File {self.query} could not be removed")

    # The following to methods are only required if the class inherits from
    # StorageObjectGlob.

    @retry_decorator
    def list_candidate_matches(self) -> Iterable[str]:
        """Return a list of candidate matches in the storage for the query."""
        # This is used by glob_wildcards() to find matches for wildcards in the query.
        # The method has to return concretized queries without any remaining wildcards.
        # Use snakemake_executor_plugins.io.get_constant_prefix(self.query) to get the
        # prefix of the query before the first wildcard.
        ...
