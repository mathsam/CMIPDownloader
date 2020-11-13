import urllib.request
import json
import random
import multiprocessing
import os
import re
import argparse
from datetime import datetime


def get_files_to_download(url_files_search):
    with urllib.request.urlopen(url_files_search) as result:
        data = json.loads(result.read().decode())
        data_docs = data['response']['docs']
        file_urls_to_download = []
        for doc_i in range(len(data_docs)):
            file_urls_to_download.extend(data_docs[doc_i]['url'])
        for file_url_to_download in file_urls_to_download:
            if file_url_to_download.split('|')[2] == 'HTTPServer':
                files_to_download.append(file_url_to_download.split('|')[0])


def download_file(url_to_download, variable_name, index, output_dir):

    print('\t Downloading file [' + str(index) + '/' + str(len(files_to_download)) +'] ' + url_to_download)

    for currentRun in range(0, 6):
        result_code = os.system('wget -nc -c --retry-connrefused --waitretry=10 --quiet -o /dev/null -P ' +
                                os.path.join(output_dir, variable_name) + ' ' + url_to_download )
        if result_code == 0:
            break

    if result_code != 0:
        print('Failed to download ' + url_to_download)
        failed_files.append(url_to_download)


if __name__ == '__main__':

    files_to_download = multiprocessing.Manager().list()
    failed_files = multiprocessing.Manager().list()
    min_datetime = datetime(1850, 1, 1)

    parser = argparse.ArgumentParser()
    parser.add_argument("-v", "--variable", required=True, help="variable name: tas, pr", type=str)
    parser.add_argument("-f", "--frequency", required=True, help="frequency: mon, ", type=str)
    parser.add_argument("-e", "--experiment", required=True, help="experiment id: amip-piForcing", type=str)
    parser.add_argument("-o", "--output_dir", default="./", help="output directory, default to current directory",
                        type=str)
    parser.add_argument("-c", "--cimp_version", default=6, type=int, choices=[5, 6], help="CIMP version: 5 or 6")
    parser.add_argument("-n", "--num_workers", default=4, type=int, help="number of workers to download in parallel")

    args = parser.parse_args()
    variable_name = args.variable
    frequency_value = args.frequency
    experiment_id = args.experiment
    cimp_version = args.cimp_version
    number_of_processes = args.num_workers

    if not os.path.exists(args.output_dir):
        os.makedirs(args.output_dir, exist_ok=True)
    print("Saving data to %s" %args.output_dir)

    variable = ''
    if variable_name:
        if args.cimp_version == 6:
            variable = '&variable_id=' + variable_name
        elif args.cimp_version == 5:
            variable = '&variable=' + variable_name
    else:
        variable_name = 'all'

    frequency = ''
    if frequency_value:
        if args.cimp_version == 6:
            frequency = '&frequency=' + frequency_value
        elif args.cimp_version == 5:
            frequency = '&time_frequency=' + frequency_value
    else:
        frequency_value = 'all'

    experiment = ''
    if experiment_id:
        if args.cimp_version == 6:
            experiment = '&experiment_id=' + experiment_id
        elif args.cimp_version == 5:
            experiment = '&experiment=' + experiment_id
    else:
        experiment_id = 'all'

    if args.cimp_version == 6:
        facets = '&facets=mip_era%2Cactivity_id%2Cmodel_cohort%2Cproduct%2Csource_id%2Cinstitution_id%2Csource_type%2Cnominal_resolution%2Cexperiment_id%2Csub_experiment_id%2Cvariant_label%2Cgrid_label%2Ctable_id%2Cfrequency%2Crealm%2Cvariable_id%2Ccf_standard_name%2Cdata_node&format=application%2Fsolr%2Bjson'
    elif args.cimp_version == 5:
        facets = '&facets=project%2Cproduct%2Cinstitute%2Cmodel%2Cexperiment%2Cexperiment_family%2Ctime_frequency%2Crealm%2Ccmor_table%2Censemble%2Cvariable%2Cvariable%2Ccf_standard_name%2Cdata_node&format=application%2Fsolr%2Bjson'

    url = 'https://esgf-node.llnl.gov/esg-search/search/' + \
         ('?offset=0&limit=10000&type=Dataset&replica=false&latest=true&project=CMIP%d&' %cimp_version) \
          + variable + frequency + experiment + facets


    print('1- Searching for records...')
    pool_search = multiprocessing.Pool(number_of_processes)
    with urllib.request.urlopen(url) as result_search:
        data = json.loads(result_search.read().decode())
        print('2- ' + str(len(data['response']['docs'])) + ' records found. Searching for files to download inside each record...')
        for doc in data['response']['docs']:
            url_files_search = 'https://esgf-node.llnl.gov/search_files/' + doc['id'] + '/' + doc['index_node'] + '/?limit=10000&rnd=' + str(random.randint(100000, 999999))
            pool_search.apply_async(get_files_to_download, args=[url_files_search])

        pool_search.close()
        pool_search.join()
        # ignore files the end before min_date
        fs = list(files_to_download)
        if cimp_version == 5:
            print("Filtering files containing variable name %s from %d files"
                  %(args.variable, len(files_to_download)))
            files_to_download = [x for x in files_to_download if args.variable.lower() in x.lower()]
            print("After filtering, %d files left" %len(files_to_download))

        fs_sub = []
        for f_i in fs:
           if len(re.split('[. -]', f_i)[-2]) == 10:
              end_date = datetime.strptime(re.split('[. -]', f_i)[-2],'%Y%m%d%H')
           elif len(re.split('[. -]', f_i)[-2]) == 6:
              end_date = datetime.strptime(re.split('[. -]', f_i)[-2],'%Y%m')
           else:
              end_date = datetime.strptime(re.split('[. -]', f_i)[-2],'%Y%m%d%H%M')

           if end_date >= min_datetime:
              fs_sub.append(f_i)
        files_to_download = fs_sub[:]

        print('3- Writing list of files ' + variable_name + '_' + frequency_value + '_' + experiment_id + '_files_url_list.txt')
        with open(variable_name + '_' + frequency_value + '_' + experiment_id + '_files_url_list.txt', 'w') as file:
            for file_to_download in files_to_download:
                file.write(file_to_download + '\n')
            file.close()

        print('4- Downloading files...')
        pool_download = multiprocessing.Pool(number_of_processes)
        index = 1
        for file_to_download in files_to_download:
            pool_download.apply_async(download_file,
                                      args=[file_to_download,
                                            variable_name + '_' + frequency_value + '_' + experiment_id,
                                            index,
                                            args.output_dir])
            index += 1
        pool_download.close()
        pool_download.join()

        print('Done :)')

        if len(failed_files) > 0:
            print('The script was not able to download some files (you can try running the script again):')
            for failed_file in failed_files:
                print(failed_file)
