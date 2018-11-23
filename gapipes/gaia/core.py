import os
import io
import logging
import six
import getpass
import requests
from requests.exceptions import HTTPError
import pandas as pd

from astropy.table import Table
from astropy import units
from astropy.extern.six.moves.urllib_parse import urljoin, urlparse

from . import utils
from .utils import Job, parse_html_response_error


logger = logging.getLogger(__name__)


__all__ = ['Tap', 'GaiaTapPlus']


class Tap(object):
    """
    Table Acess Protocol service client

    Parameters
    ----------
    host : str, required
        host name
    path : str, mandatory
        server context
    protocol : str, optional
        access protocol, usually 'http' or 'https'
    port : int, optional, default '80'
        HTTP port
    """
    _tables = None

    def __init__(self, host, path, protocol='http', port=80):
        self.protocol = protocol
        self.host = host
        self.path = path
        self.port = port
        self.session = requests.session()

        logger.debug('TAP: {:s}'.format(self.tap_endpoint))
    
    @property
    def tap_endpoint(self):
        return urljoin("{s.protocol:s}://{s.host:s}".format(s=self), self.path)
    
    @staticmethod
    def parse_tableset(xml):
        """Parse vod:tableset XML and return a list of tables

        Parameters
        ----------
        xml : str
            XML string to be parsed
        
        Returns
        -------
        tableset : TableSet
            list of tables
        """
        return utils.parse_tableset(xml)

    # TODO: actually found this to return private tables, too when logged in.
    @property
    def tables(self):
        """
        List of available tables
        """
        if self._tables is None:
            response = self.session.get("{s.tap_endpoint}/tables".format(s=self))
            if not response.raise_for_status():
                tables = Tap.parse_tableset(response.text)
                self._tables = tables
                return tables
        else:
            return self._tables

    def _post_query(self, query, name=None,
                    upload_resource=None, upload_table_name=None,
                    output_format='csv',
                    autorun=True,
                    async_=False):
        """POST synchronous or asynchronos query to Tap server

        Parameters
        ----------
        query : str
            query to be executed
        async_ : bool
            send asynchronous query if True
        upload_resource: str, optional
            resource to be uploaded to UPLOAD_SCHEMA
        upload_table_name: str, required if upload_resource is
            provided, default None
            resource temporary table name associated to the uploaded resource

        Returns
        -------
        response : requests.Response
            response
        """
        # TODO: docstring is missing what other output_format is available.
        args = {
            "REQUEST": "doQuery",
            "LANG": "ADQL",
            "FORMAT": str(output_format),
            # "tapclient": str(TAP_CLIENT_ID),
            "QUERY": str(query)}
        # TODO: is autorun even necessary for sync jobs?
        if autorun is True:
            args['PHASE'] = 'RUN'
        if name is not None:
            args['jobname'] = name
        url = self.tap_endpoint + ('/async' if async_ else '/sync')
        logger.debug(args)
        
        if upload_resource is None:
            response = self.session.post(url, data=args)
        else:
            if upload_table_name is None:
                raise ValueError("Table name is required when a resource " +
                                 "is uploaded")
            # UPLOAD should be '[table_name],param:form_key'
            args['UPLOAD'] = '{0:s},param:{0:s}'.format(upload_table_name)
            if isinstance(upload_resource, Table):
                with io.BytesIO() as f:
                    upload_resource.write(f, format='votable')
                    f.seek(0)
                    chunk = f.read()
            else:
                with open(upload_resource, "r") as f:
                    chunk = f.read()
            files = {upload_table_name: chunk}
            response = self.session.post(url, data=args, files=files)

        if not response.raise_for_status():
            return response
    
    def query(self, query, name=None, upload_resource=None, upload_table_name=None,
              output_format='csv'):
        """
        Synchronous query to TAP server

        Parameters
        ----------
        query : str
            query to be executed
        name : str, optional
            job name
        upload_resource: str, optional
            resource to be uploaded to UPLOAD_SCHEMA
        upload_table_name: str, required if upload_resource is
            provided, default None
            resource temporary table name associated to the uploaded resource
        output_format : str, optional
            one of 'votable', 'votable_plain', 'csv', 'json' or 'fits'

        Returns
        -------
        #TODO
        table : astropy Table
            Query result
        """
        r = self._post_query(
            query, name=name, upload_resource=upload_resource,
            upload_table_name=upload_table_name, output_format=output_format)
        #TODO: may add option for astropy table
        return pd.read_csv(io.StringIO(r.text))

    def query_async(self, query, name=None,
                    upload_resource=None, upload_table_name=None,
                    output_format="votable",
                    autorun=True):
        """
        Do asynchronous query to server

        Parameters
        ----------
        query : str, mandatory
            query to be executed
        upload_resource: str, optional, default None
            resource to be uploaded to UPLOAD_SCHEMA
        upload_table_name: str, required if upload_resource is
            provided, default None
            resource temporary table name associated to the uploaded resource
        output_format : str, optional, default 'votable'
            results format
        autorun: boolean, optional, default True
            if 'True', sets 'phase' parameter to 'RUN',
            so the framework can start the job.

        Returns
        -------
        job : 
            blah
        """
        #NOTE: The first response is 303 redirect to Job location
        # Job location is in the header of redirect response
        r = self._post_query(
            query, name=name, upload_resource=upload_resource,
            upload_table_name=upload_table_name, output_format=output_format,
            async_=True)
        return Job.from_response(r)

    @classmethod
    def from_url(cls, url, **kwargs):
        """
        Make a Tap from url [http(s)://]host[:port][/path]
        """
        default_port = {'http': 80, 'https':443}
        if '://' not in url:
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
        return '{cls:s}("{s.host:s}", "{s.path:s}", "{s.protocol:s}", {s.port:d})'\
            .format(s=self, cls=self.__class__.__name__)


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
    table_edit_context : str, optional, default None
        context for all actions to be performed over a existing table
    data_context : str, optional, default None
        data context
    datalink_context : str, optional, default None
        datalink context
    verbose : bool, optional, default 'True'
        flag to display information about the process
    """
    def __init__(self, host, path, protocol='http', port=80,
                 server_context=None, upload_context=None):
                #  table_edit_context=None, data_context=None,
                #  datalink_context=None):

        super(GaiaTapPlus, self).__init__(host, path, protocol=protocol, port=port)

        if not all([v is not None for v in [server_context, upload_context]]):
            raise ValueError(
                "It does not make sense to initialize `TapPlus`"
                "without all contexts set. Consider using `Tap`.")

        self.server_context = server_context
        self.upload_context = upload_context
        # self.table_edit_context = table_edit_context
        # self.data_context = data_context
        # self.datalink_context = datalink_context
    
    def login(self, user=None, password=None, credentials_file=None,
              verbose=False):
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
        verbose : bool, optional, default 'False'
            flag to display information about the process
        """
        if credentials_file is not None:
            # read file: get user & password
            with open(credentials_file, "r") as ins:
                user = ins.readline().strip()
                password = ins.readline().strip()
        if user is None:
            user = six.moves.input("User: ")
            if user is None:
                print("Invalid user name")
                return
        if password is None:
            password = getpass.getpass("Password: ")
            if password is None:
                print("Invalid password")
                return
        url = "https://{s.host:s}/{s.server_context:s}/login".format(s=self)
        r = self.session.post(url, data={'username': user, 'password': password})
        if not r.raise_for_status():
            return
    
    def logout(self):
        """
        Logout from TAP server
        """
        url = "https://{s.host:s}/{s.server_context:s}/logout".format(s=self)
        r = self.session.post(url)
        if not r.raise_for_status():
            return
    
    @property
    def baseurl(self):
        return '{s.protocol:s}://{s.host:s}/{s.server_context:s}'.format(s=self)

    def get_table_info(self, tables=None, only_tables=False, share_accessible=False):
        """
        Load all accessible tables

        Parameters
        ----------
        tables : str
            comma-separated name of schema.tables to query
        only_tables : bool
            True to get table names only
        share_accessible : bool
            True to include shared tables

        Returns
        -------
        tables : TableSet 
            list of tables
        """
        url = "{s.tap_endpoint}/tables".format(s=self)
        logger.debug("tables url = {:s}".format(url))

        payload = dict(
            tables=tables,
            only_tables=only_tables,
            share_accessible=True if include_shared_tables else False
        )
        r = self.session.get(url, params=payload)

        if not r.raise_for_status():
            tsp = TableSaxParser()
            # TODO: this is a stopgap
            tsp.parseData(io.BytesIO(r.content))
        return tsp.get_tables()

    def upload_table(self, upload_resource, table_name,
                     table_description="",
                     format='votable'):
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
        # TODO: available froamts from http://gea.esac.esa.int/archive-help/index.html
        #       where else is TapPlus applicable?
        url = "{s.baseurl:s}/{s.upload_context}".format(s=self)
        # url = "https://gea.esac.esa.int/tap-server/Upload"
        logger.debug("upload_table url = {:s}".format(url))
        # TODO: WOW. It actaully seems necessary to pass on TASKID
        # Otherwise you will get 500 Internal server error.
        # This is confirmed even with curl in command line.
        # This is not docummented in gaia help page.
        args = {
            'TASKID': str(-1),
            'TABLE_NAME': table_name,
            'TABLE_DESC': table_description,
            'FORMAT': format
        }
        if isinstance(upload_resource, Table):
            with io.BytesIO() as f:
                upload_resource.write(f, format='votable')
                f.seek(0)
                chunk = f.read()
                args['FORMAT'] = 'votable'
                files = dict(FILE=chunk)
        elif upload_resource.startswith('http'):
            files = None
            args['URL'] = upload_resource
        else:
            with open(upload_resource, "r") as f:
                chunk = f.read()
            files = dict(FILE=chunk)
        r = self.session.post(url, data=args, files=files)
        return r

    def delete_user_table(self, table_name, force_removal=False):
        """Delete a user table

        Parameters
        ----------
        table_name: str
            table to be removed
        force_removal : bool, optional
            flag to indicate if removal should be forced
        """
        url = "{s.baseurl:s}/{s.upload_context}".format(s=self)

        # TODO: what does force removal mean?
        args = {
            "TABLE_NAME": str(table_name),
            "DELETE": "TRUE",
            "FORCE_REMOVAL": "TRUE" if force_removal else "FALSE"
        }

        r = self.session.post(url, data=args)
        if not r.raise_for_status():
            return r



# class GaiaClass(TapPlus):

#     """
#     Proxy class to default TapPlus object (pointing to Gaia Archive)
#     """
#     MAIN_GAIA_TABLE = conf.MAIN_GAIA_TABLE
#     MAIN_GAIA_TABLE_RA = conf.MAIN_GAIA_TABLE_RA
#     MAIN_GAIA_TABLE_DEC = conf.MAIN_GAIA_TABLE_DEC

#     def __init__(self, tap_plus_conn_handler=None, datalink_handler=None):
#         super(GaiaClass, self).__init__(url="http://gea.esac.esa.int/",
#                                         server_context="tap-server",
#                                         tap_context="tap",
#                                         upload_context="Upload",
#                                         table_edit_context="TableTool",
#                                         data_context="data",
#                                         datalink_context="datalink",
#                                         connhandler=tap_plus_conn_handler)
#         # Data uses a different TapPlus connection
#         if datalink_handler is None:
#             self.__gaiadata = TapPlus(url="http://geadata.esac.esa.int/",
#                                       server_context="data-server",
#                                       tap_context="tap",
#                                       upload_context="Upload",
#                                       table_edit_context="TableTool",
#                                       data_context="data",
#                                       datalink_context="datalink")
#         else:
#             self.__gaiadata = datalink_handler

#     def load_data(self, ids, retrieval_type="epoch_photometry",
#                   valid_data=True, band=None, format="VOTABLE",
#                   output_file=None, verbose=False):
#         """Loads the specified table
#         TAP+ only

#         Parameters
#         ----------
#         ids : str list, mandatory
#             list of identifiers
#         retrieval_type : str, optional, default 'epoch_photometry'
#             retrieval type identifier
#         valid_data : bool, optional, default True
#             By default, the epoch photometry service returns only valid data,
#             that is, all data rows where flux is not null and
#             rejected_by_photometry flag is not true. In order to retrieve
#             all data associated to a given source without this filter,
#             this request parameter should be included (valid_data=False)
#         band : str, optional, default None, valid values: G, BP, RP
#             By default, the epoch photometry service returns all the
#             available photometry bands for the requested source.
#             This parameter allows to filter the output lightcurve by its band.
#         format : str, optional, default 'votable'
#             loading format
#         output_file : string, optional, default None
#             file where the results are saved.
#             If it is not provided, the http response contents are returned.
#         verbose : bool, optional, default 'False'
#             flag to display information about the process

#         Returns
#         -------
#         A table object
#         """
#         if retrieval_type is None:
#             raise ValueError("Missing mandatory argument 'retrieval_type'")
#         if ids is None:
#             raise ValueError("Missing mandatory argument 'ids'")
#         params_dict = {}
#         if valid_data:
#             params_dict['VALID_DATA'] = "true"
#         else:
#             params_dict['VALID_DATA'] = "false"
#         if band is not None:
#             if band != 'G' and band != 'BP' and band != 'RP':
#                 raise ValueError("Invalid band value '%s' (Valid values: " +
#                                  "'G', 'BP' and 'RP)" % band)
#             else:
#                 params_dict['BAND'] = band
#         if isinstance(ids, six.string_types):
#             ids_arg = ids
#         else:
#             if isinstance(ids, int):
#                 ids_arg = str(ids)
#             else:
#                 ids_arg = ','.join(str(item) for item in ids)
#         params_dict['ID'] = ids_arg
#         params_dict['FORMAT'] = str(format)
#         params_dict['RETRIEVAL_TYPE'] = str(retrieval_type)
#         return self.__gaiadata.load_data(params_dict=params_dict,
#                                          output_file=output_file,
#                                          verbose=verbose)

#     def get_datalinks(self, ids, verbose=False):
#         """Gets datalinks associated to the provided identifiers
#         TAP+ only

#         Parameters
#         ----------
#         ids : str list, mandatory
#             list of identifiers
#         verbose : bool, optional, default 'False'
#             flag to display information about the process

#         Returns
#         -------
#         A table object
#         """
#         return self.__gaiadata.get_datalinks(ids=ids, verbose=verbose)

#     def __query_object(self, coordinate, radius=None, width=None, height=None,
#                        async_job=False, verbose=False):
#         """Launches a job
#         TAP & TAP+

#         Parameters
#         ----------
#         coordinate : astropy.coordinate, mandatory
#             coordinates center point
#         radius : astropy.units, required if no 'width' nor 'height' are
#         provided
#             radius (deg)
#         width : astropy.units, required if no 'radius' is provided
#             box width
#         height : astropy.units, required if no 'radius' is provided
#             box height
#         async_job : bool, optional, default 'False'
#             executes the query (job) in asynchronous/synchronous mode (default
#             synchronous)
#         verbose : bool, optional, default 'False'
#             flag to display information about the process

#         Returns
#         -------
#         The job results (astropy.table).
#         """
#         coord = self.__getCoordInput(coordinate, "coordinate")
#         job = None
#         if radius is not None:
#             job = self.__cone_search(coord, radius,
#                                      async_job=async_job, verbose=verbose)
#         else:
#             raHours, dec = commons.coord_to_radec(coord)
#             ra = raHours * 15.0  # Converts to degrees
#             widthQuantity = self.__getQuantityInput(width, "width")
#             heightQuantity = self.__getQuantityInput(height, "height")
#             widthDeg = widthQuantity.to(units.deg)
#             heightDeg = heightQuantity.to(units.deg)
#             query = "SELECT DISTANCE(POINT('ICRS',"\
#                     "" + str(self.MAIN_GAIA_TABLE_RA) + ","\
#                     "" + str(self.MAIN_GAIA_TABLE_DEC) + ")"\
#                     ", POINT('ICRS'," + str(ra) + "," + str(dec) + ""\
#                     ")) AS dist, * FROM " + str(self.MAIN_GAIA_TABLE) + ""\
#                     " WHERE CONTAINS(POINT('ICRS'"\
#                     "," + str(self.MAIN_GAIA_TABLE_RA) + ","\
#                     "" + str(self.MAIN_GAIA_TABLE_DEC) + "),BOX('ICRS"\
#                     "'," + str(ra) + "," + str(dec) + ", "\
#                     "" + str(widthDeg.value) + ","\
#                     " " + str(heightDeg.value) + ""\
#                     "))=1 ORDER BY dist ASC"
#             if async_job:
#                 job = self.launch_job_async(query, verbose=verbose)
#             else:
#                 job = self.launch_job(query, verbose=verbose)
#         return job.get_results()

#     def query_object(self, coordinate, radius=None, width=None, height=None,
#                      verbose=False):
#         """Launches a job
#         TAP & TAP+

#         Parameters
#         ----------
#         coordinate : astropy.coordinates, mandatory
#             coordinates center point
#         radius : astropy.units, required if no 'width' nor 'height' are
#         provided
#             radius (deg)
#         width : astropy.units, required if no 'radius' is provided
#             box width
#         height : astropy.units, required if no 'radius' is provided
#             box height
#         verbose : bool, optional, default 'False'
#             flag to display information about the process

#         Returns
#         -------
#         The job results (astropy.table).
#         """
#         return self.__query_object(coordinate, radius, width, height,
#                                    async_job=False, verbose=verbose)

#     def query_object_async(self, coordinate, radius=None, width=None,
#                            height=None, verbose=False):
#         """Launches a job (async)
#         TAP & TAP+

#         Parameters
#         ----------
#         coordinate : astropy.coordinates, mandatory
#             coordinates center point
#         radius : astropy.units, required if no 'width' nor 'height' are
#         provided
#             radius
#         width : astropy.units, required if no 'radius' is provided
#             box width
#         height : astropy.units, required if no 'radius' is provided
#             box height
#         async_job : bool, optional, default 'False'
#             executes the query (job) in asynchronous/synchronous mode
#             (default synchronous)
#         verbose : bool, optional, default 'False'
#             flag to display information about the process

#         Returns
#         -------
#         The job results (astropy.table).
#         """
#         return self.__query_object(coordinate, radius, width, height,
#                                    async_job=True, verbose=verbose)

#     def __cone_search(self, coordinate, radius, table_name=MAIN_GAIA_TABLE,
#                       ra_column_name=MAIN_GAIA_TABLE_RA,
#                       dec_column_name=MAIN_GAIA_TABLE_DEC,
#                       async_job=False,
#                       background=False,
#                       output_file=None, output_format="votable", verbose=False,
#                       dump_to_file=False):
#         """Cone search sorted by distance
#         TAP & TAP+

#         Parameters
#         ----------
#         coordinate : astropy.coordinate, mandatory
#             coordinates center point
#         radius : astropy.units, mandatory
#             radius
#         table_name: str, optional, default main gaia table
#             table name doing the cone search against
#         ra_column_name: str, optional, default ra column in main gaia table
#             ra column doing the cone search against
#         dec_column_name: str, optional, default dec column in main gaia table
#             dec column doing the cone search against
#         async_job : bool, optional, default 'False'
#             executes the job in asynchronous/synchronous mode (default
#             synchronous)
#         background : bool, optional, default 'False'
#             when the job is executed in asynchronous mode, this flag specifies
#             whether the execution will wait until results are available
#         output_file : str, optional, default None
#             file name where the results are saved if dumpToFile is True.
#             If this parameter is not provided, the jobid is used instead
#         output_format : str, optional, default 'votable'
#             results format
#         verbose : bool, optional, default 'False'
#             flag to display information about the process
#         dump_to_file : bool, optional, default 'False'
#             if True, the results are saved in a file instead of using memory

#         Returns
#         -------
#         A Job object
#         """
#         coord = self.__getCoordInput(coordinate, "coordinate")
#         raHours, dec = commons.coord_to_radec(coord)
#         ra = raHours * 15.0  # Converts to degrees
#         if radius is not None:
#             radiusQuantity = self.__getQuantityInput(radius, "radius")
#             radiusDeg = commons.radius_to_unit(radiusQuantity, unit='deg')
#         query = "SELECT DISTANCE(POINT('ICRS',"+str(ra_column_name)+","\
#             + str(dec_column_name)+"), \
#             POINT('ICRS',"+str(ra)+","+str(dec)+")) AS dist, * \
#             FROM "+str(table_name)+" WHERE CONTAINS(\
#             POINT('ICRS',"+str(ra_column_name)+","+str(dec_column_name)+"),\
#             CIRCLE('ICRS',"+str(ra)+","+str(dec)+", "+str(radiusDeg)+"))=1 \
#             ORDER BY dist ASC"
#         if async_job:
#             return self.launch_job_async(query=query,
#                                          output_file=output_file,
#                                          output_format=output_format,
#                                          verbose=verbose,
#                                          dump_to_file=dump_to_file,
#                                          background=background)
#         else:
#             return self.launch_job(query=query,
#                                    output_file=output_file,
#                                    output_format=output_format,
#                                    verbose=verbose,
#                                    dump_to_file=dump_to_file)

#     def cone_search(self, coordinate, radius=None,
#                     table_name=MAIN_GAIA_TABLE,
#                     ra_column_name=MAIN_GAIA_TABLE_RA,
#                     dec_column_name=MAIN_GAIA_TABLE_DEC,
#                     output_file=None,
#                     output_format="votable", verbose=False,
#                     dump_to_file=False):
#         """Cone search sorted by distance (sync.)
#         TAP & TAP+

#         Parameters
#         ----------
#         coordinate : astropy.coordinate, mandatory
#             coordinates center point
#         radius : astropy.units, mandatory
#             radius
#         table_name: str, optional, default main gaia table
#             table name doing the cone search against
#         ra_column_name: str, optional, default ra column in main gaia table
#             ra column doing the cone search against
#         dec_column_name: str, optional, default dec column in main gaia table
#             dec column doing the cone search against
#         output_file : str, optional, default None
#             file name where the results are saved if dumpToFile is True.
#             If this parameter is not provided, the jobid is used instead
#         output_format : str, optional, default 'votable'
#             results format
#         verbose : bool, optional, default 'False'
#             flag to display information about the process
#         dump_to_file : bool, optional, default 'False'
#             if True, the results are saved in a file instead of using memory

#         Returns
#         -------
#         A Job object
#         """
#         return self.__cone_search(coordinate,
#                                   radius=radius,
#                                   table_name=table_name,
#                                   ra_column_name=ra_column_name,
#                                   dec_column_name=dec_column_name,
#                                   async_job=False,
#                                   background=False,
#                                   output_file=output_file,
#                                   output_format=output_format,
#                                   verbose=verbose,
#                                   dump_to_file=dump_to_file)

#     def cone_search_async(self, coordinate, radius=None,
#                           table_name=MAIN_GAIA_TABLE,
#                           ra_column_name=MAIN_GAIA_TABLE_RA,
#                           dec_column_name=MAIN_GAIA_TABLE_DEC,
#                           background=False,
#                           output_file=None, output_format="votable",
#                           verbose=False, dump_to_file=False):
#         """Cone search sorted by distance (async)
#         TAP & TAP+

#         Parameters
#         ----------
#         coordinate : astropy.coordinate, mandatory
#             coordinates center point
#         radius : astropy.units, mandatory
#             radius
#         table_name: str, optional, default main gaia table
#             table name doing the cone search against
#         ra_column_name: str, optional, default ra column in main gaia table
#             ra column doing the cone search against
#         dec_column_name: str, optional, default dec column in main gaia table
#             dec column doing the cone search against
#         background : bool, optional, default 'False'
#             when the job is executed in asynchronous mode, this flag
#             specifies whether
#             the execution will wait until results are available
#         output_file : str, optional, default None
#             file name where the results are saved if dumpToFile is True.
#             If this parameter is not provided, the jobid is used instead
#         output_format : str, optional, default 'votable'
#             results format
#         verbose : bool, optional, default 'False'
#             flag to display information about the process
#         dump_to_file : bool, optional, default 'False'
#             if True, the results are saved in a file instead of using memory

#         Returns
#         -------
#         A Job object
#         """
#         return self.__cone_search(coordinate,
#                                   radius=radius,
#                                   table_name=table_name,
#                                   ra_column_name=ra_column_name,
#                                   dec_column_name=dec_column_name,
#                                   async_job=True,
#                                   background=background,
#                                   output_file=output_file,
#                                   output_format=output_format,
#                                   verbose=verbose,
#                                   dump_to_file=dump_to_file)

#     def __checkQuantityInput(self, value, msg):
#         if not (isinstance(value, str) or isinstance(value, units.Quantity)):
#             raise ValueError(
#                 str(msg) + " must be either a string or astropy.coordinates")

#     def __getQuantityInput(self, value, msg):
#         if value is None:
#             raise ValueError("Missing required argument: '"+str(msg)+"'")
#         if not (isinstance(value, str) or isinstance(value, units.Quantity)):
#             raise ValueError(
#                 str(msg) + " must be either a string or astropy.coordinates")
#         if isinstance(value, str):
#             q = Quantity(value)
#             return q
#         else:
#             return value

#     def __checkCoordInput(self, value, msg):
#         if not (isinstance(value, str) or isinstance(value,
#                                                      commons.CoordClasses)):
#             raise ValueError(
#                 str(msg) + " must be either a string or astropy.coordinates")

#     def __getCoordInput(self, value, msg):
#         if not (isinstance(value, str) or isinstance(value,
#                                                      commons.CoordClasses)):
#             raise ValueError(
#                 str(msg) + " must be either a string or astropy.coordinates")
#         if isinstance(value, str):
#             c = commons.parse_coordinates(value)
#             return c
#         else:
#             return value

#     def cross_match(self, full_qualified_table_name_a=None,
#                     full_qualified_table_name_b=None,
#                     results_table_name=None,
#                     radius=1.0,
#                     background=False,
#                     verbose=False):
#         """Performs a cross match between the specified tables
#         The result is a join table (stored in the user storage area)
#         with the identifies of both tables and the distance.
#         TAP+ only

#         Parameters
#         ----------
#         full_qualified_table_name_a : str, mandatory
#             a full qualified table name (i.e. schema name and table name)
#         full_qualified_table_name_b : str, mandatory
#             a full qualified table name (i.e. schema name and table name)
#         results_table_name : str, mandatory
#             a table name without schema. The schema is set to the user one
#         radius : float (arc. seconds), optional, default 1.0
#             radius  (valid range: 0.1-10.0)
#         verbose : bool, optional, default 'False'
#             flag to display information about the process

#         Returns
#         -------
#         Boolean indicating if the specified user is valid
#         """
#         if full_qualified_table_name_a is None:
#             raise ValueError("Table name A argument is mandatory")
#         if full_qualified_table_name_b is None:
#             raise ValueError("Table name B argument is mandatory")
#         if results_table_name is None:
#             raise ValueError("Results table name argument is mandatory")
#         if radius < 0.1 or radius > 10.0:
#             raise ValueError("Invalid radius value. Found " + str(radius) +
#                              ", valid range is: 0.1 to 10.0")
#         schemaA = taputils.get_schema_name(full_qualified_table_name_a)
#         if schemaA is None:
#             raise ValueError("Not found schema name in " +
#                              "full qualified table A: '" +
#                              full_qualified_table_name_a + "'")
#         tableA = taputils.get_table_name(full_qualified_table_name_a)
#         schemaB = taputils.get_schema_name(full_qualified_table_name_b)
#         if schemaB is None:
#             raise ValueError("Not found schema name in " +
#                              "full qualified table B: '" +
#                              full_qualified_table_name_b + "'")
#         tableB = taputils.get_table_name(full_qualified_table_name_b)
#         if taputils.get_schema_name(results_table_name) is not None:
#             raise ValueError("Please, do not specify schema for " +
#                              "'results_table_name'")
#         query = "SELECT crossmatch_positional(\
#             '"+schemaA+"','"+tableA+"',\
#             '"+schemaB+"','"+tableB+"',\
#             "+str(radius)+",\
#             '"+str(results_table_name)+"')\
#             FROM dual;"
#         name = str(results_table_name)
#         return self.launch_job_async(query=query,
#                                      name=name,
#                                      output_file=None,
#                                      output_format="votable",
#                                      verbose=verbose,
#                                      dump_to_file=False,
#                                      background=background,
#                                      upload_resource=None,
#                                      upload_table_name=None)


# Gaia = GaiaClass()
