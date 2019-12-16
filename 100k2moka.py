"""
v1.0 - AB 2019/12/11
Requirements:
    ODBC connection to Moka
    Python 3.6
    pyodbc

usage: 100k2moka.py [-h] -i INPUT_FILE -o OUTPUT_FILE

Parses output from negneg_cases.py and books all cases into Moka

optional arguments:
  -h, --help            show this help message and exit
  -i INPUT_FILE, --input_file INPUT_FILE
                        output from negneg_cases.py
  -o OUTPUT_FILE, --output_file OUTPUT_FILE
                        tab-separated log file
"""
import argparse
from configparser import ConfigParser
import os
import sys
import socket
import pyodbc
import datetime

# Read config file (must be called config.ini and stored in same directory as script)
config = ConfigParser()
config.read(os.path.join(os.path.dirname(os.path.realpath(__file__)), "config.ini"))

def process_arguments():
    """
    Uses argparse module to define and handle command line input arguments and help menu
    """
    # Create ArgumentParser object. Description message will be displayed as part of help message if script is run with -h flag
    parser = argparse.ArgumentParser(description='Parses output from negneg_cases.py and books all cases into Moka')
    # Define the arguments that will be taken.
    parser.add_argument('-i', '--input_file', required=True, help='output from negneg_cases.py')
    parser.add_argument('-o', '--output_file', required=True, help='tab-separated log file')
    # Return the arguments
    return parser.parse_args()

class MokaConnector(object):
    """
    Connection to Moka database for use by other functions
    """
    def __init__(self):
        self.cnxn = pyodbc.connect(
            f'DRIVER={{ODBC Driver 17 for SQL Server}}; SERVER={config.get("MOKA", "SERVER")}; DATABASE={config.get("MOKA", "DATABASE")}; '
            f'UID={config.get("MOKA", "USER")}; PWD={config.get("MOKA", "PASSWORD")}', 
            autocommit=True
            )
        self.cursor = self.cnxn.cursor()

    def __del__(self):
        self.cnxn.close()

class Case100kMoka(object):
    """
    Represents a 100k case. Instantiated using a GeL participant ID, interpretation request ID (<irid>-<version>), genome assembly and string containing any GeL case flags
    """
    def __init__(self, participantID, intrequestID, assembly, flags):
        self.participantID = participantID
        self.intrequestID = intrequestID
        self.assembly = assembly
        self.flags = flags 
        self.proband_100k_rows = []
        self.internalPatientID = None
        self.patient_status = None
        self.clinicianID = None
        self.pru = None
        self.ngstests = []


    def get_moka_patientIDs(self, cursor):
        """
        Get information from Moka related to the proband 
        """
        sql = f"SELECT InternalPatientID, Referring_Clinician, PatientTrustID FROM Probands_100k WHERE Participant_ID = '{self.participantID}'"
        self.proband_100k_rows = cursor.execute(sql).fetchall()
        # Only update attributes if a single matching record is found.
        if len(self.proband_100k_rows) == 1:
            self.internalPatientID = self.proband_100k_rows[0].InternalPatientID
            self.clinicianID = self.proband_100k_rows[0].Referring_Clinician
            self.pru = self.proband_100k_rows[0].PatientTrustID

    def get_patient_status(self, cursor):
        """
        Get the patient status from Moka
        """
        if self.internalPatientID:
            sql = f"SELECT s_StatusOverall FROM Patients WHERE InternalPatientID = {self.internalPatientID}"
            self.patient_status = cursor.execute(sql).fetchone().s_StatusOverall

    def get_moka_ngstests(self, cursor):
        """
        Get list of matching 100k NGS test records from Moka
        """
        # Only execute if internal patient ID is known.
        if self.internalPatientID:
            sql = (
                "SELECT NGSTestID, StatusID, IRID, GELProbandID, ResultCode, BookBy, Check1ID, Check1Date, "
                f"BlockAutomatedReporting FROM dbo.NGSTest WHERE InternalPatientID = {self.internalPatientID} AND ReferralID = 1199901218"
                )
            # Capture matching NGSTests
            self.ngstests = cursor.execute(sql).fetchall()
                 
    def get_moka_details(self, cursor):
        """
        Execute functions to retrieve case details from Moka
        """
        self.get_moka_patientIDs(cursor)
        self.get_moka_ngstests(cursor)
        self.get_patient_status(cursor)

    def add_ngstest(self, cursor):
        """
        Create an NGStest in Moka for the case
        """
        # If patient status is currently Complete, update it to 100K
        # If it's any other status, just leave the patient status as it is (in case it is also having other testing in the lab) and skip ahead to creating the NGS test.
        if self.patient_status == 4:
            sql = f"UPDATE Patients SET s_StatusOverall = 1202218839 WHERE InternalPatientID = {self.internalPatientID}"
            cursor.execute(sql)
            # Record in patient log
            # Use the name of the script as 'Login' and the server hostname as 'PCName'
            sql = (
                "INSERT INTO PatientLog (InternalPatientID, LogEntry, Date, Login, PCName) "
                f"VALUES ({self.internalPatientID}, 'Patients: Status changed to 100K', '{datetime.datetime.now().strftime(r'%Y%m%d %H:%M:%S %p')}', "
                f"'{os.path.basename(__file__)}', '{socket.gethostname()}');"
                )
            cursor.execute(sql)
        # Create NGStest and record in patient log
        # Convert genome build to ID from Moka Item table. Should always be either GRCh38 or GRCh37, but if anything else record as 'Unknown'
        if self.assembly == 'GRCh38':
            build_id = 3224
        elif self.assembly == 'GRCh37':
            build_id = 109
        else:
            build_id = 289
        if self.flags:
            flags_sql = f"'{self.flags}'"
        else:
            flags_sql = "Null"
        # Use N/A as 'BookingAuthorisedByID' since it doesn't really apply here
        sql = (
            "INSERT INTO NGSTest (InternalPatientID, ReferralID, StatusID, DateRequested, BookBy, ResultBuild, BookingAuthorisedByID, Service, GELProbandID, IRID, GeL_case_flags) "
            f"Values ({self.internalPatientID}, 1199901218, 2, '{datetime.datetime.now().strftime(r'%Y%m%d %H:%M:%S %p')}', '{self.clinicianID}', {build_id}, "
            f"1201865434, 0, '{self.participantID}', '{self.intrequestID}', {flags_sql});"
            )
        cursor.execute(sql)
        # Record in patient log
        # Use the name of the script as 'Login' and the server hostname as 'PCName'
        sql = (
            "INSERT INTO PatientLog (InternalPatientID, LogEntry, Date, Login, PCName) "
            f"VALUES ({self.internalPatientID}, 'NGS: GeL test request added.', '{datetime.datetime.now().strftime(r'%Y%m%d %H:%M:%S %p')}', "
            f"'{os.path.basename(__file__)}', '{socket.gethostname()}');"
            )
        cursor.execute(sql)
        
def print_log(log_file, participantid, irid, pru, status, message):
    with open(log_file, 'a') as file_obj:
        file_obj.write(f"{participantid}\t{irid}\t{pru}\t{status}\t{message}\n")

def book_in_moka(cases, mokaconn, log_file):
    # Print header for output
    print_log(log_file, 'GeLParticipantID', 'InterpretationRequestID', 'PRU', 'Status', 'Log')
    for case in cases:
        case.get_moka_details(mokaconn.cursor)
        # Check that Moka internal patient ID and referring clinician ID has been found for this case, if not print error to log and skip to next case
        if not case.internalPatientID or not case.clinicianID:
            print_log(log_file, case.participantID, case.intrequestID, case.pru, "ERROR", "No Moka InternalPatientID and/or referring clinician found for this patient, check they are in Moka Probands_100k table")
        # If there is already an NGStest request in Moka for this interpretation request, skip to next case
        elif case.intrequestID in [ngstest.IRID for ngstest in case.ngstests]:
            print_log(log_file, case.participantID, case.intrequestID, case.pru, "SKIP", "NGSTest request already exists for this interpretation request ID")
        # If there are currently no NGStest requests in Moka, create one from scratch...
        else:
            case.add_ngstest(mokaconn.cursor)
            print_log(log_file, case.participantID, case.intrequestID, case.pru, "SUCCESS", "Created new NGSTest request")

def main():
    # Get command line arguments
    args = process_arguments()
    # Raise error if file doesn't start with expected header row
    with open(args.input_file, 'r') as file_to_check:
        if not file_to_check.read().startswith('participant_ID\tCIP_ID\tassembly\tflags\tgroup'):
            sys.exit('Input file does not contain expected header row. Exiting')
    # Create a list of 100k case objects
    cases = []
    with open(args.input_file, 'r') as case_list:
        for case in case_list:
            if not case.startswith('participant_ID'):
                participantID = case.split('\t')[0]
                intrequestID = case.split('\t')[1]
                assembly = case.split('\t')[2]
                flags = case.split('\t')[3]
                cases.append(Case100kMoka(participantID, intrequestID, assembly, flags))
    # Create a Moka connection
    mokaconn = MokaConnector()
    # Book cases into moka
    book_in_moka(cases, mokaconn, args.output_file)


if __name__ == '__main__':
    main()
