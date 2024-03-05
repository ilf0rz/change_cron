from csv import DictReader, DictWriter
import requests 
import asyncio
import aiohttp
import os
import sys
import argparse

# Columns used to write CSV log file
output_columns = ['Name', 'App Name', 'Old Schedule', 'New Schedule', 'Status', 'Details']

# Columns expected for the input file
input_columns = ['savedsearch','old','new','app']

# disable insecure HTTPS requests due to Verify=False setting
requests.packages.urllib3.disable_warnings()

# ANSI escape codes for some colors
class Colors:
    OK = '\033[92m\033[1m'
    BLUE = '\033[94m\033[1m'
    CYAN = '\033[36m\033[1m'
    PURPLE = '\033[35m\033[1m'    
    WARNING = '\033[93m\033[1m'
    FAIL = '\033[91m\033[1m'
    ENDC = '\033[0m'

# Enhanced function to handle multiple escape sequences
def parse_delimiter_arg(value):
    escape_sequences = {
        '\\t': '\t',  # Tab
        '\\n': '\n',  # Newline
        '\\r': '\r',  # Carriage return
        '\\\\': '\\',  # Backslash
        # Add more escape sequences as needed
    }
    # Replace the escape sequence with its actual value
    for seq, actual in escape_sequences.items():
        if value == seq:
            return actual
    return value

# Get saved searches using requests 
def get_saved_searches(base_url=None, username=None, password=None, disabled=False):
    with requests.Session() as s:
        s.auth = (username,password) 
        s.verify = False
        #savedsearchedreq = s.get(url=f"{base_url}/services/configs/conf-savedsearches", params={'count':"-1"}, data={"output_mode": "json"})
        savedsearchedreq = s.get(url=f"{base_url}/servicesNS/-/-/saved/searches", params={'count':"-1"}, data={"output_mode": "json"})
        
        if savedsearchedreq.status_code == 200:
            if disabled is not None:
                savedsearch = [ x for x in savedsearchedreq.json()['entry'] if x['content']['disabled'] == disabled ]
            else:
                savedsearch = savedsearchedreq.json()['entry']
            return savedsearch
        else:
            raise Exception("Failed fetching saved searches.")

# wraps gather implementing a semaphore in order to avoid killing Splunk Instance with too many requests to process.
async def gather_with_concurrency(n, *coros):
    semaphore = asyncio.Semaphore(n)

    async def sem_coro(coro):
        async with semaphore:
            return await coro

    return await asyncio.gather(*(sem_coro(c) for c in coros),return_exceptions=True)

async def reschedule(session, savedsearch, change, dryrun, outputpath, rollback):
    source = 'new' if rollback else 'old'
    target = 'old' if rollback else 'new'
    
    if not dryrun:
        async with  session.post(f"{savedsearch['id']}", data={"output_mode": "json","cron_schedule": change[target].strip()}) as response:
            results = await response.json()
            if response.status == 200:
                print(f"# {Colors.OK}SUCCESS{Colors.ENDC}: Savedsearch \'{Colors.PURPLE}{savedsearch['name']}{Colors.ENDC}\' rescheduled from \'{Colors.CYAN}{savedsearch['content']['cron_schedule']}{Colors.ENDC}\' to \'{Colors.CYAN}{change[target].strip()}{Colors.ENDC}\'.")
                log_status = 'Success'
            else:
                details = await response.text()
                print(f"# {Colors.FAIL}ERROR{Colors.FAIL}: Savedsearch \'{Colors.PURPLE}{savedsearch['name']}{Colors.ENDC}\' failed to reschedule\'.")
                log_status = 'Error'
    else:
        log_status = 'dryrun'

    # Prepare log line to be saved to CSV
    log_dict = {
        'Name': savedsearch['name'],
        'App Name': change['app'].strip(),
        'Old Schedule': savedsearch['content']['cron_schedule'],
        'New Schedule': change[target].strip(),
        'Status': log_status,
        'Details': details if log_status == 'Error' else ''
    }

    # Writing log to  CSV
    with open(outputpath, 'a', newline='') as csvfile:
        writer = DictWriter(csvfile, fieldnames=output_columns)
        writer.writerow(log_dict)
    return results

# create tasks list to be executed concurrently
async def create_tasklist(base_url=None, username=None, password=None, disabled=False, changes={},dryrun=True,rollback=None, outputpath=None, concurrency=8):
    
    source = 'new' if rollback else 'old'
    target = 'old' if rollback else 'new'

    tasks = []
    print(f'# {Colors.BLUE}INFO{Colors.ENDC}: Fetching savedsearches from splunkd=\'{Colors.CYAN}{base_url}{Colors.ENDC}\'.') 
    savedsearches = get_saved_searches(base_url=base_url, username=username, password=password, disabled=disabled)
    savedsearches = { x['name']: x for x in savedsearches }
    print(f'# {Colors.OK}SUCCESS{Colors.ENDC}: Done fetching len=\'{Colors.CYAN}{len(savedsearches)}{Colors.ENDC}\' savedsearches from splunkd=\'{Colors.CYAN}{base_url}{Colors.ENDC}\'.') 
    async with aiohttp.ClientSession(auth=aiohttp.BasicAuth(username, password), connector=aiohttp.TCPConnector(ssl=False)) as session:
        for change in changes:
            if change['savedsearch'] in savedsearches:
                if change[target].strip() == savedsearches[change['savedsearch']]['content']['cron_schedule']:
                    print(f"# {Colors.WARNING}WARNING{Colors.ENDC}: Savedsearch \'{Colors.PURPLE}{change['savedsearch']}{Colors.ENDC}\' already correctly scheduled.") 
                    log_dict = {
                        'Name': change['savedsearch'],
                        'App Name': change['app'].strip(),
                        'Old Schedule': savedsearches[change['savedsearch']]['content']['cron_schedule'],
                        'New Schedule': change[target].strip(),
                        'Status': 'Warning' if not dryrun else 'dryrun',
                        'Details': f"Search '{change['savedsearch']}' already correcrly scheduled."
                    }

                    with open(outputpath, 'a', newline='') as csvfile:
                        writer = DictWriter(csvfile, fieldnames=output_columns)
                        writer.writerow(log_dict)              
                    continue
                else:
                    task = reschedule(session, savedsearches[change['savedsearch']], change, dryrun, outputpath, rollback)
            else:
                print(f"# {Colors.FAIL}ERROR{Colors.ENDC}: Failed changing schedule for search '{Colors.PURPLE}{change['savedsearch']}{Colors.ENDC}' from '{Colors.CYAN}{change[source].strip()}{Colors.ENDC}' to '{Colors.CYAN}{change[target].strip()}{Colors.ENDC}'. Details: savedsearch not found on splunkd.") 

                log_dict = {
                    'Name': change['savedsearch'],
                    'App Name': change['app'].strip(),
                    'Old Schedule': change[source].strip(),
                    'New Schedule': change[target].strip(),
                    'Status': 'Error' if not dryrun else 'dryrun',
                    'Details': f"Search '{change['savedsearch']}' not found on splunkd."
                }

                with open(outputpath, 'a', newline='') as csvfile:
                    writer = DictWriter(csvfile, fieldnames=output_columns)
                    writer.writerow(log_dict)

                continue
            tasks.append(task)

        print(f'# {Colors.BLUE}INFO{Colors.ENDC}: Number of tasks to be processed asyncronously: task=\'{Colors.CYAN}{len(tasks)}{Colors.ENDC}\'.')

        _ = await gather_with_concurrency(concurrency, *tasks)

def main():

    # Setup argument parser
    parser = argparse.ArgumentParser(description='Script to change scheduling of saved searches in Splunk.')

    # Adding arguments with short versions
    parser.add_argument('-b', '--base_url', type=str, required=True, help='Base URL for the Splunk instance (e.g.: https://splunk.local:8089/).')
    parser.add_argument('-u', '--username', type=str, required=True, help='Username for Splunk authentication.')
    parser.add_argument('-p', '--password', type=str, required=True, help='Password for Splunk authentication.')
    parser.add_argument('-i', '--inputcsvpath', type=str, required=True, help='Path to the input CSV file containing at least: "savedserch","old","new".')
    parser.add_argument('-o', '--outputcsvpath', type=str, required=True, help='Path to the output CSV file.')
    parser.add_argument('-d', '--delimiter', type=parse_delimiter_arg, default=',', help='CSV delimiter. Use escape sequences like \\t for tab, \\n for newline, etc.')
    parser.add_argument('-c', '--concurrency', type=int, default=8, help='Concurrency level for asynchronous tasks. Defaults to 8.')
    parser.add_argument('-r', '--dryrun', action='store_true', help='Perform a dry run without making any changes.')
    parser.add_argument('-rb', '--rollback', action='store_true', help='Perform a dry run without making any changes.')
    parser.add_argument('--disabled', action='store_true', help='Filter out disabled rules.')


    # Parse arguments
    args = parser.parse_args()

    # Using parsed arguments in your script
    # Example usage with new "disabled" parameter handling
    base_url = args.base_url
    username = args.username
    password = args.password
    inputcsvpath = args.inputcsvpath
    outputpath = args.outputcsvpath
    delimiter = args.delimiter
    concurrency = args.concurrency
    dryrun = args.dryrun
    rollback = args.rollback
    disabled = args.disabled

    if disabled:
        print(f"# {Colors.WARNING}WARNING{Colors.ENDC}: disabled searches will not be taken into account.")
    if dryrun:
        print(f"# {Colors.WARNING}WARNING{Colors.ENDC}: {Colors.CYAN}DRYRUN{Colors.ENDC} mode detected. No change will be made to scheduling.")
    if rollback:
        print(f"# {Colors.WARNING}WARNING{Colors.ENDC}: {Colors.CYAN}ROLLBACK{Colors.ENDC} mode detected. We will be moving from {Colors.CYAN}new{Colors.ENDC} -> {Colors.CYAN}old{Colors.ENDC}.")

    if os.path.exists(outputpath):
        os.remove(outputpath)
        print(f"# File {outputpath} detected. Removing it.")

    # Preparing CSV file used for output
    with open(outputpath, 'w') as fout:
        writer = DictWriter(fout, fieldnames=output_columns)
        writer.writeheader()

    with open(inputcsvpath,'r') as f:
        reader = DictReader(f,delimiter=delimiter)
        # Verifica che le colonne richieste siano presenti
        if not all(col in reader.fieldnames for col in input_columns):
            print(f"# {Colors.FAIL}ERROR{Colors.ENDC}: Columns 'old', 'new', 'savedsearch' not detected in input file.")
            sys.exit()        
        changes = list(reader)

    # real main code that kickoff async execution.
    asyncio.run(
        create_tasklist(
            base_url=base_url, 
            username=username, 
            password=password, 
            disabled=disabled, 
            changes=changes, 
            dryrun=dryrun,
            rollback=rollback,
            outputpath=outputpath,
            concurrency=concurrency
        )
    )
if __name__ == '__main__':
    main()
