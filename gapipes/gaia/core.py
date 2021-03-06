import warnings
import io
import logging
from urllib.parse import urljoin, urlparse
import getpass
import requests
from requests.exceptions import HTTPError
import pandas as pd
from astropy.table import Table

from . import utils
from .utils import Job, parse_html_error_response, parse_votable_error_response


logger = logging.getLogger(__name__)


__all__ = ["Tap", "GaiaTapPlus"]


class QueryError(Exception):
    pass


class Tap(object):
    """
    Table Acess Protocol service client

    Parameters
    ----------
    host : str
        host name
    path : str
        server context
    protocol : str
        access protocol, usually 'http' or 'https'
    port : int
        HTTP port; Default: 80 for http and 443 for https
    """

    _tables = None
    _columns = None

    def __init__(self, host, path, protocol="http", port=80):
        self.protocol = protocol
        self.host = host
        self.path = path
        self.port = port
        self.session = requests.session()

        logger.debug("TAP: {:s}".format(self.tap_endpoint))

    @property
    def tap_endpoint(self):
        return urljoin("{s.protocol:s}://{s.host:s}".format(s=self), self.path)

    @staticmethod
    def parse_tableset(xml):
        """Parse vod:tableset XML and return a list of (tables, columns)

        Parameters
        ----------
        xml : str
            XML string to be parsed

        Returns
        -------
        tables, columns : pandas.DataFrame
            list of tables and columns for all available tables
        """
        return utils.parse_tableset(xml)

    @staticmethod
    def parse_result_table(response, format):
        """Parse the returned table according to its format

        Parameters
        ----------
        response : requests.Response
            response to query from server
        format : str
            the table format, e.g., 'csv'
        """
        if format not in ["votable", "csv", "fits"]:
            raise ValueError("format is not recognized")
        if format == "csv":
            return pd.read_csv(io.StringIO(response.text))
        elif format == "votable":
            # suppress warnings by default
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                return Table.read(io.BytesIO(response.content), format="votable")
        elif format == "fits":
            # TODO: this is broken but I don't know why.
            return Table.read(io.BytesIO(response.content), format="fits")

    @property
    def tables(self):
        """
        List of available tables
        """
        if self._tables is None:
            response = self.session.get("{s.tap_endpoint}/tables".format(s=self))
            if not response.raise_for_status():
                tables, columns = Tap.parse_tableset(response.text)
                self._tables = tables
                self._columns = columns
                return tables
        else:
            return self._tables

    @property
    def columns(self):
        """
        List of columns for all tables
        """
        if self._tables is None:
            response = self.session.get("{s.tap_endpoint}/tables".format(s=self))
            if not response.raise_for_status():
                tables, columns = Tap.parse_tableset(response.text)
                self._tables = tables
                self._columns = columns
                return columns
        else:
            return self._columns

    def _post_query(
        self,
        query,
        name=None,
        upload_resource=None,
        upload_table_name=None,
        output_format="csv",
        autorun=True,
        async_=False,
    ):
        """POST synchronous or asynchronos query to Tap server

        Returns unchecked requests.Response
        """
        if "select" not in query.lower():
            with open(query, "r") as f:
                query = f.read()
        args = {
            "REQUEST": "doQuery",
            "LANG": "ADQL",
            "FORMAT": str(output_format),
            # "tapclient": str(TAP_CLIENT_ID),
            "QUERY": str(query),
        }
        # TODO: is autorun even necessary for sync jobs?
        if autorun is True:
            args["PHASE"] = "RUN"
        if name is not None:
            args["jobname"] = name
        url = self.tap_endpoint + ("/async" if async_ else "/sync")
        logger.debug(args)

        if upload_resource is None:
            response = self.session.post(url, data=args)
        else:
            if upload_table_name is None:
                raise ValueError(
                    "Table name is required when a resource " + "is uploaded"
                )
            # UPLOAD should be '[table_name],param:form_key'
            args["UPLOAD"] = "{0:s},param:{0:s}".format(upload_table_name)
            if isinstance(upload_resource, pd.DataFrame):
                with io.BytesIO() as f:
                    Table.from_pandas(upload_resource).write(f, format="votable")
                    f.seek(0)
                    chunk = f.read()
            elif isinstance(upload_resource, Table):
                with io.BytesIO() as f:
                    upload_resource.write(f, format="votable")
                    f.seek(0)
                    chunk = f.read()
            else:
                with open(upload_resource, "r") as f:
                    chunk = f.read()
            files = {upload_table_name: chunk}
            response = self.session.post(url, data=args, files=files)

        return response

    def query(
        self,
        query,
        name=None,
        upload_resource=None,
        upload_table_name=None,
        output_format="csv",
        async_=False,
    ):
        """Send query to TAP server

        Parameters
        ----------
        query : str
            query or path containing query
        name : str, optional
            job name
        upload_resource: path to votable file or pandas.DataFrame or astropy.table.Table
            table to upload
        upload_table_name: str
            upload table name
        output_format : str
            one of 'votable', 'votable_plain', 'csv', 'json' or 'fits'
        async_ : bool
            True to launch an asynchronous job

        Returns
        -------
        The return type depends on whether you launch a synchronous or asynchronous query.

        For synchronous queries:
        table : pd.DataFrame or astropy.table.Table
            Query result
        
        For asynchronous queries:
        job : Job instance
            use `job.get_result()` to retrieve query result
        """
        r = self._post_query(
            query,
            name=name,
            upload_resource=upload_resource,
            upload_table_name=upload_table_name,
            output_format=output_format,
            async_=async_,
        )
        try:
            r.raise_for_status()
            if not async_:
                if r.text:
                    return Tap.parse_result_table(r, output_format)
                else:
                    # NOTE: GaiaArchive has an upstream bug that nothing is returned
                    #       when synchronous queries time out (30 seconds).
                    raise QueryError(
                        "Your synchronous query returned nothing; it probably timed out."
                    )
            else:
                # NOTE: The first response is 303 redirect to Job location
                # Job location is in the header of redirect response
                return Job.from_response(r, session=self.session)
        except HTTPError as e:
            message = parse_votable_error_response(r)
            raise HTTPError(message) from e

    @classmethod
    def from_url(cls, url, **kwargs):
        """
        Make a Tap from url [http(s)://]host[:port][/path]
        """
        default_port = {"http": 80, "https": 443}
        if "://" not in url:
            raise ValueError('`url` must start with "scheme://"')
        parsed_url = urlparse(url)
        protocol = parsed_url.scheme
        host = parsed_url.hostname
        port = parsed_url.port
        if not port:
            port = default_port[protocol]
        path = parsed_url.path
        return cls(host, path, protocol=protocol, port=port, **kwargs)

    def __repr__(self):
        return '{cls:s}("{s.host:s}", "{s.path:s}", "{s.protocol:s}", {s.port:d})'.format(
            s=self, cls=self.__class__.__name__
        )


class GaiaTapPlus(Tap):
    """
    Gaia TAP+ Service

    Parameters
    ----------
    host : str, optional, default None
        host name
    server_context : str, optional, default None
        server context
    upload_context : str, optional, default None
        upload context
    """

    def __init__(
        self,
        host,
        path,
        protocol="http",
        port=80,
        server_context=None,
        upload_context=None,
    ):

        super(GaiaTapPlus, self).__init__(host, path, protocol=protocol, port=port)

        if not all([v is not None for v in [server_context, upload_context]]):
            raise ValueError(
                "It does not make sense to initialize `TapPlus`"
                "without all contexts set. Consider using `Tap`."
            )

        self._server_context = server_context
        self._upload_context = upload_context

    def login(self, user=None, password=None, credentials_file=None):
        """
        Login to TAP server

        User and password arguments can be used or a file that contains user
        name and password (2 lines: one for user name and the following one
        for the password). If no arguments are provided, a prompt asking for
        user name and password will appear.

        Parameters
        ----------
        user : str, default None
            login name
        password : str, default None
            user password
        credentials_file : str, default None
            file containing user and password in two lines
        """
        if credentials_file is not None:
            # read file: get user & password
            with open(credentials_file, "r") as ins:
                user = ins.readline().strip()
                password = ins.readline().strip()
        if user is None:
            user = input("User: ")
            if user is None:
                print("Invalid user name")
                return
        if password is None:
            password = getpass.getpass("Password: ")
            if password is None:
                print("Invalid password")
                return
        url = "https://{s.host:s}/{s._server_context:s}/login".format(s=self)
        r = self.session.post(url, data={"username": user, "password": password})
        try:
            r.raise_for_status()
            return
        except HTTPError as e:
            message = parse_html_error_response(r.text)
            raise HTTPError(message) from e

    def logout(self):
        """
        Logout from TAP server
        """
        url = "https://{s.host:s}/{s._server_context:s}/logout".format(s=self)
        r = self.session.post(url)
        try:
            r.raise_for_status()
            return
        except HTTPError as e:
            message = "Logout failed: are you sure you were logged in?"
            raise HTTPError(message) from e

    @property
    def baseurl(self):
        return "{s.protocol:s}://{s.host:s}/{s._server_context:s}".format(s=self)

    def get_table_info(self, tables=None, only_tables=False, share_accessible=False):
        """
        Get table metadata for accessible tables

        Parameters
        ----------
        tables : str
            comma-separated name of schema.tables to query
            If None, list of all available tables is returned. This includes
            user tables when logged in.
        only_tables : bool
            True to get table names only
        share_accessible : bool
            True to include shared tables

        Returns
        -------
        tables, columns : pandas.DataFrame
            list of tables and columns


        .. note::
            `only_tables` is only effective when listing all tables (i.e., tables=None)

        .. note::
            The result depends on whether you are logged in or not.
            If you are logged in, your user tables will also be included.
        """
        url = "{s.tap_endpoint}/tables".format(s=self)
        logger.debug("tables url = {:s}".format(url))

        payload = dict(
            tables=tables,
            only_tables=only_tables,
            share_accessible=True if share_accessible else False,
        )
        r = self.session.get(url, params=payload)

        try:
            r.raise_for_status()
            tables, columns = self.parse_tableset(r.text)
            return tables, columns
        except HTTPError as e:
            message = parse_html_error_response(r.text)
            raise HTTPError(message) from e

    # TODO: doument all options of upload_resource better.
    # TODO: make it flexible to handle pandas.DataFrame seamlessly.
    # TODO: test all options of upload_resource works.
    def upload_table(
        self, upload_resource, table_name, table_description="", format="votable"
    ):
        """
        Upload a table to the user private space

        Parameters
        ----------
        upload_resource : object
            table to be uploaded: pyTable, file or URL.
        table_name: str
            table name associated to the uploaded resource
        table_description: str, optional
            table description
        format : str, optional
            resource format
            Available formats: 'VOTable', 'CSV' and 'ASCII'
        """
        url = "{s.baseurl:s}/{s._upload_context}".format(s=self)
        # url = "https://gea.esac.esa.int/tap-server/Upload"
        logger.debug("upload_table url = {:s}".format(url))
        args = {
            "TABLE_NAME": table_name,
            "TABLE_DESC": table_description,
            "FORMAT": format,
        }
        if isinstance(upload_resource, Table):
            with io.BytesIO() as f:
                upload_resource.write(f, format="votable")
                f.seek(0)
                chunk = f.read()
                args["FORMAT"] = "votable"
                files = dict(FILE=chunk)
        elif upload_resource.startswith("http"):
            files = None
            args["URL"] = upload_resource
        else:
            with open(upload_resource, "r") as f:
                chunk = f.read()
            files = dict(FILE=chunk)
        r = self.session.post(url, data=args, files=files)
        try:
            r.raise_for_status()
            return r.text.strip()
        except HTTPError as e:
            message = parse_html_error_response(r.text)
            raise HTTPError(message) from e

    def delete_user_table(self, table_name, force_removal=False):
        """Delete a user table

        Parameters
        ----------
        table_name: str
            table to be removed
        force_removal : bool, optional
            flag to indicate if removal should be forced
        """
        url = "{s.baseurl:s}/{s._upload_context}".format(s=self)

        # TODO: what does force removal mean?
        args = {
            "TABLE_NAME": str(table_name),
            "DELETE": "TRUE",
            "FORCE_REMOVAL": "TRUE" if force_removal else "FALSE",
        }

        r = self.session.post(url, data=args)
        try:
            r.raise_for_status()
            return r.text.strip()
        except HTTPError as e:
            message = parse_html_error_response(r.text)
            raise HTTPError(message) from e

    def list_jobs(self, kind="async", offset=0, limit=100):
        """Get the list of jobs from server

        Parameters
        ----------
        kind : str, optional
            'sync' or 'async'
        offset : int, optional
            number of jobs to skip
        limit : int, optional
            maximum number of jobs to return

        Returns
        -------
        pandas.DataFrame
            list of jobs


        .. note::
            The result depends on whether you are logged in or not.
            If you are logged in, only your jobs will be returned.
            If you are not logged in, all anonymous jobs will be returned.
        """
        # NOTE: /jobs/sync returns nothing...
        if kind not in ["sync", "async"]:
            raise ValueError("`kind` must be one of 'sync' or 'async'")
        args = {"OFFSET": offset, "LIMIT": limit}
        url = "{s.tap_endpoint:s}/jobs/{kind:s}".format(s=self, kind=kind)
        logger.debug(url)

        r = self.session.get(url, params=args)
        try:
            r.raise_for_status()
            import xml.etree.ElementTree as ET

            root = ET.fromstring(r.content)
            jobs = list(map(Job.parse_xml, root))
            return pd.DataFrame(jobs)
        except HTTPError as e:
            message = parse_html_error_response(r.text)
            raise HTTPError(message) from e

    def query_sourceid(self, table, source_id_column="source_id", columns="*"):
        """Query Gaia DR2 gaia_source table for a given list of "source_id"s.

        Parameters
        ----------
        table : pd.DataFrame, astropy.table.Table
            table containing source ids
        source_id_column : str, optional
            name of the column containing source_id
        columns : list, optional
            List of columns to retrieve
            (the default is '*', which will get all columns)
        """

        q = """SELECT {columns} FROM TAP_UPLOAD.table as t JOIN gaiadr2.gaia_source
        ON gaiadr2.gaia_source.source_id = t.{source_id_column}""".format(
            columns=columns, source_id_column=source_id_column
        )
        r = self.query(str(q), upload_resource=table, upload_table_name="table")
        return r
