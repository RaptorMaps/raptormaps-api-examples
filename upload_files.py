#!/usr/bin/env python

"""upload_files.py: A working example of how to uploads files using the
Raptor Maps API.

This script assumes the directory you choose to upload has solar sites
as the first subdirectory.

An Upload Request is created for each solar site, and Upload Sessions
are added to each Upload Request. This script uses a multi-threaded
approach within each Upload Session.

Args:
    org_id (int): Your Raptor App org id", type=int)
    image_dir (str): Path to image file directory
    -p, --prefix (str): Optional prefix on each upload session name.
    -n, --n_workers (int): Optional number or worker threads for uploading 
        default=6
    -h (flag): Help flag to display params

Example:

> python upload_files.py /Users/username/images -n 6

If the following directory structure was used

-- images
    -- site_name_1
        -- YYYY-MM-DD
            -- high_fly
               -- IMG_0001.jpg
            -- module_scan
               -- 100MEDIA
                  -- IMG_0001.jpg
                  -- ...
                   -- IMG_0999.jpg
               -- 101MEDIA
                   -- IMG_0001.jpg
                   -- ...
                   -- IMG_0999.jpg
            -- pads_poi
                -- IMG_0001.jpg
    -- site_name_2
        -- YYYY-MM-DD
            -- high_fly
            -- module_scan
               -- 100MEDIA
               -- 101MEDIA
            -- pads_poi

This script would create 2 upload requests: one for site 1 and one for site 2.
It would then create 5 upload sessions for site 1 and 4 for site 2.

Modification:
It is best practice to create one Upload Request per solar site. See main(), but
get_solar_site_dirs() and get_upload_session_dirs() can be modified if needed.
"""

__copyright__ = "Raptor Maps Inc. 2020 (c)"

import argparse
import concurrent.futures
import getpass
import glob
import json
import os
import requests
import time

BASE_URL = "https://app.raptormaps.com"

def login(server_path, email, password):
    """Generic login function. Returns cookie and auth token for Raptor Maps
    Arguments:
        server_path (str): e.g. https://app.raptormaps.com
        email (str): e.g. gavinbelson@raptormaps.com
        password (str): e.g. 123456789
    Returns:
        {session: (hashed_session_string)}, <auth_token> (str)
    """

    data = {"email":str(email),"password":str(password)}

    login_path = server_path + '/login'
    headers = {"Content-Type": "application/json"}

    r = requests.post(login_path, headers=headers, data=json.dumps(data))

    # find the session cookie
    for c in r.cookies:
        if c.name == 'session':
            cookie = {c.name: c.value}

    response = r.json()
    auth_token = response["response"]["user"]["authentication_token"]

    return cookie, auth_token

class UploadRequest(object):
    """UploadRequest class handles creating an upload request with the
    Raptor Maps API and uploading files to AWS S3
    """

    def __init__(self, org_id, name, auth_token):
        """Constructor
        org_id (int): your Org's ID
        name (str): name of upload request
        auth_token (str): Raptor Maps API auth token
        """
        self.org_id = org_id
        self.name = name

        # API request headers
        self.headers = {
            'content-type': 'application/json',
            'Authentication-Token': auth_token}

    def create(self):
        # First create an upload request
        endpoint = "/api/v2/upload_requests"
        url = BASE_URL + endpoint

        payload = {
            'org_id': self.org_id,
            'name': self.name
        }

        # POST
        r = requests.post(url, data=json.dumps(payload), headers=self.headers)

        data = r.json()

        # Get the access token for the upload request
        access_token = data['upload_request']['access_token']['access_token']

        return access_token


class UploadSession(object):
    """UploadSession class handles creating an upload session with the
    Raptor Maps API and uploading files to AWS S3
    """
    def __init__(
            self, file_dir, session_name, org_id, access_token, n_workers=6):
        """Constructor.
        Args:
            file_dir (str): local directory path where images are stored to be
                uploaded
            session_name (str): name of upload session
            org_id (int): your org's ID
            cookie (str): authentication token
            n_workers (int): number of threads to use when uploading, default=6
        """

        self.file_dir = file_dir
        self.filepaths = self.get_filepaths(file_dir)
        self.session_name = session_name
        self.org_id = org_id
        self.access_token = access_token
        self.n_workers = n_workers

        # total file count for upload session
        self.total_file_count = len(self.filepaths)

        # API request headers
        self.headers = {
            'content-type': 'application/json'
        }

        # Initialize variables
        self.upload_session_id = None
        self.counter = 0

    @staticmethod
    def get_filepaths(file_dir):
        """Generic method to gets a list of filepaths from a file directory

        Args:
            file_dir (str) a path to a file directory

        Returns:
            [str]. a list of filepaths for each file

        """

        # Accepted file extensions
        accepted_exts = ['jpg', 'jpeg', 'tif', 'tiff']

        filepaths = []

        # Find files with accepted file extensions
        for e in accepted_exts:
            filepaths.extend(glob.glob(os.path.join(
                file_dir, '*.%s' % e)))

            # Also look for files with uppercase extension like JPG
            filepaths.extend(glob.glob(os.path.join(
                file_dir, '*.%s' % e.upper())))

        # Sort files
        filepaths.sort()

        return filepaths

    @staticmethod
    def get_total_files_in_dir(file_dir):
        """Gets total number of files in a directory
        Args:
            file_dir (str): file directory
        Returns:
            total number of files (int)
        """
        # Get count of all files recursively in all subdirectories
        total_files = 0
        for root,d_names,f_names in os.walk(file_dir):
            total_files += len(UploadSession.get_filepaths(root))

        return total_files

    def upload_file(self, filepath):
        """Uploads one file.
        First it asks the API for where to place the file on S3, then it uploads
        the file to S3. It then triggers the Raptor Maps system to ingest the
        file and peform post processing
        """

        # Determine filename from filepath
        filename = os.path.basename(filepath)

        # Get AWS S3 post url so we can upload directly to S3
        endpoint = "/api/v2/token/%s/get_s3_post_link" % (self.access_token)
        url = BASE_URL + endpoint

        payload = {
            'upload_session_id': self.upload_session_id,
            'filename': filename
        }

        # POST
        r = requests.post(url, data=json.dumps(payload), headers=self.headers)

        # parse response
        data = r.json()

        if int(data['exit_status']) == 0:
            post = data['post']
        else:
            raise

        # Upload file directly to s3
        try:
            self.post_file_to_s3(filepath, post)
        except ConnectionError as e:
            print('ConnectionError:', e)
            return

        # Initialize data ingestion for this file
        endpoint = "/api/v2/token/%s/upload_file" % (self.access_token)
        url = BASE_URL + endpoint

        payload = {
            'upload_session_id': self.upload_session_id,
            's3_url': post['url'],
            'data_type': 'image' # or geotiff
        }

        # POST
        r = requests.post(url, data=json.dumps(payload), headers=self.headers)

        print('------ Session %s: %s of %s: %s' % (
            self.upload_session_id, self.counter+1, self.total_file_count,
            os.path.basename(filepath)))

        self.counter += 1


    def post_file_to_s3(self, filepath, post, retry_period=10, retry_duration=7200):
        """Uploads a single file to AWS S3. If the post is unsuccessful
        it will retry every 10 seconds for 2 hours

        Args:
            filepath (str): filepath
            post (dict): post dictionary from S3 {url: (str), fields: dict}
            retry_period: number of seconds to wait before each rety
            retry_duration: total length of time to keep retrying (in seconds)

        Raises:
            ConnectionError
        """

        # Parse fields
        url = post['url']
        fields = post['fields']

        # Open file io
        files = {'file': open(filepath, 'rb')}

        # Attempt to upload file bytes
        for attempt in range(retry_duration):
            try:
                r_s3 = requests.post(url, data=fields, files=files)
            except:
                print('...Trying to get to S3...')
                time.sleep(retry_period)
            else:
                break
        else:
            # Loop never hit break, i.e the request never went
            # through
            raise ConnectionError('ERROR: Skipping file: %s' % (f))

    def upload_files(self):
        """Uploads many files using multi-threading by calling upload_file
        """

        with concurrent.futures.ThreadPoolExecutor(max_workers=self.n_workers) as executor:
            # Start the load operations and mark each future with its URL
            future_to_url = {
                executor.submit(
                    self.upload_file, x): x for x in self.filepaths}

            for future in concurrent.futures.as_completed(future_to_url):
                url = future_to_url[future] # object originally passed
                data = future.result() # get data from function

    def create_upload_session(self):
        """Creates an upload session in Raptor Maps API
        First creates an upload session request for each upload session.
        The upload request contains a token to use for subsequent API calls
        """

        # Create Upload Session
        endpoint = "/api/v2/token/%s/upload_sessions" % (self.access_token)
        url = BASE_URL + endpoint

        payload = {
            'file_total': len(self.filepaths),
            'name': self.session_name,
        }

        # POST
        r = requests.post(url, data=json.dumps(payload), headers=self.headers)

        # Make two attempts to create an upload session
        try:
            data = r.json()
        except ValueError:
            r = requests.post(url, data=json.dumps(payload), headers=self.headers)
            data = r.json()

        # Sets the session id
        self.upload_session_id = data['upload_session']['id']

    def run(self):
        """Convenience function to create upload session in Raptor Maps system
        and upload files to it
        """
        print('---- Uploading: %s' % self.session_name)
        self.create_upload_session()
        self.upload_files()


def get_solar_site_dirs(file_dir):
    """Gets all solar site directories

    Note: This can be customized to suit your needs

    Args:

        file_dir (str): path to a directory contain subdirectories
            that are solar sites

    Returns:
        site_dirs ([str]): list of solar site file directories
        site_names ([site_names]): list of solar site names
            extracted from file paths

    Example:
        Assuming file directory is
            /User/username/solar_farms/
                solar_farm_a/
                solar_farm_b/

        > get_solar_site_dirs('/User/username/solar_farms/')
        ['/User/username/solar_farms/solar_farm_a',
         '/User/username/solar_farms/solar_farm_b'],
        ['solar_farm_a', 'solar_farm_b']
    """

    site_dirs = []
    site_names = []

    for x in next(os.walk(file_dir))[1]:
        site_names.append(x)
        site_dirs.append(os.path.join(file_dir, x))

    # To be safe, make sure the number of directories matches the number
    # of site names
    assert len(site_dirs) == len(site_names)

    # Also check that all filepaths exist
    for site_dir in site_dirs:
        if not os.path.exists(site_dir):
            raise OSError(site_dir, 'does not exist')


    return site_dirs, site_names

def get_upload_session_dirs(image_dir, prefix=''):
    """Takes a directory that contains images or images within subdirectories
    and determines upload session directories and upload session names.

    Note: This can be customized to suit your needs

    Args:
        image_dir (str): path to directory containing images or subdirectories
            of images. e.g. '/Users/username/Downloads/images'
        prefix (str): text to append to front of upload session names.
            e.g. 'ABC'

    Returns:
        file_dirs ([str]): a list of file directories for each upload session
        session_names ([str]): a list of upload session names

    Example:
        image_dir = '/Users/username/Downloads/images'
        prefix = 'ABC'
        where the subdirectory structure looks like:
            images/
            -- high_flys/
                IMG_0001.JPG
                IMG_0002.JPG
            - module_scans/
                MEDIA01/
                    IMG_0003.JPG
                    IMG_0004.JPG

        Call: get_upload_session_dirs('/Users/username/Downloads/images', 'ABC')
        Returns:
          file_dirs: ['/Users/username/Downloads/images/high_flys']
            '/Users/username/Downloads/images/module_scans/MEDIA01'],
          session_names: ['ABC - high_flys', 'ABC - module_scans/MEDIA01'],
          total_file_countr: 4

    """

    file_dirs = []
    session_names = []
    total_file_count = 0

    # Recursively traverse the entire directory and flatten the lowest folders
    # into upload session names
    for root,d_names,f_names in os.walk(image_dir):

        filepaths = UploadSession.get_filepaths(root)

        # If there are some files make an upload session
        if len(filepaths) > 0:

            file_dirs.append(root)

            # Determine session name. If no subdirectories are found
            # just use the folder name
            if root == image_dir:
                session_name = os.path.basename(root)
            else:
                session_name = os.path.relpath(root, image_dir)

            if prefix.strip() != "":
                session_name = '%s - %s' % (prefix, session_name)

            session_names.append(session_name)

            total_file_count += len(filepaths)

    # To be safe, make sure the number of file directories matches the number
    # of upload session names
    assert len(file_dirs) == len(session_names)

    # Also check that all filepaths exist
    for file_dir in file_dirs:
        if not os.path.exists(file_dir):
            raise OSError(file_dir, 'does not exist')

    return file_dirs, session_names, total_file_count

def parse_args():
    # Create argument parser
    parser = argparse.ArgumentParser()

    # positional mandatory arguments
    parser.add_argument("org_id", help="Your Raptor App org id", type=int)
    parser.add_argument("image_dir", help="Path to image file directory", type=str)
    parser.add_argument("-p", "--prefix",
                        help="Pre-fix on session and job names. Such as SLV",
                        type=str, default="")

    parser.add_argument("-n", "--n_workers",
                        help="Number of worker threads",
                        type=int, default=6)


    # Parse arguments
    args = parser.parse_args()

    return args

def main():

    # Parse command line arguments
    args = parse_args()

    org_id = args.org_id
    image_dir = args.image_dir
    prefix = args.prefix
    n_workers = args.n_workers

    image_dir = os.path.normpath(image_dir)

    ### Authentication ###
    # Options:
    # 1. Load token from environment variable (recommended)
    # 2. Type your token into this script
    # 3. Login using your password

    # Option 1:
    auth_token = os.environ['RAPTOR_MAPS_API_TOKEN']

    # Option 2:
    #auth_token = 'abcd-1234'

    # Option 3: Log user into the API
    #email = getpass.getuser()
    #password = getpass.getpass()
    #cookie, auth_token = login(BASE_URL, email, password)
    ### END AUTHENTICATION ###

    # Get total number of files to upload
    total_n_files = UploadSession.get_total_files_in_dir(image_dir)

    # Get a list of solar sites
    solar_site_dirs, site_names = get_solar_site_dirs(image_dir)

    print("-- Total files to upload: %s" % total_n_files)
    print("-- Total sites to upload: %s" % len(site_names))
    print("-- Site Names:")

    for s in site_names:
        print('---- %s' % s)

    total_file_counter = 0

    # Loop over each solar site
    for site_dir, site_name in zip(solar_site_dirs, site_names):
        # Create an Upload Request for each solar site
        print('-- Creating Upload Request for %s' % (site_name))
        ur = UploadRequest(org_id, site_name, auth_token)
        upload_request_token = ur.create()
        print('-- Token: %s' % (upload_request_token))

        # Prepare to make upload sessions by getting a list of file directories
        # and associated upload session names for this solar site
        image_dirs, session_names, site_n_files = \
            get_upload_session_dirs(site_dir, prefix)

        print("---- %s: %s files" % (site_name, site_n_files))
        print("---- Upload Sessions:")
        for s in session_names:
            print("------ %s" % s)

        # Loop over each upload session and upload files
        for file_dir, session_name in zip(image_dirs, session_names):
            # Initialize Upload Session
            uploader = UploadSession(
                file_dir, session_name, org_id, upload_request_token,
                n_workers=n_workers)
            # Upload files
            uploader.run()

            total_file_counter = total_file_counter + uploader.counter

        print('-- Total Files Uploaded: %s of %s' % (total_file_counter, total_n_files))

if __name__ == '__main__':
    main()
