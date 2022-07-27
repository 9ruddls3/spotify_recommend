import os
import sys
import time
import pandas as pd

import multiproceesing_setting as m_setting

from multiprocessing import Pool


def create_directory(directory):
    try:
        if not os.path.exists(directory):
            os.makedirs(directory)
    except OSError:
        print("Error: Failed to create the directory.")


def CF_Recommendation(dir_list, drop_duplicates=True):

    return


if __name__ == '__main__':
    start_time = time.time()

    n_cpu = m_setting.get_num_cpus()
    m_imput = m_setting.multi_processing_setting()

    p = Pool(n_cpu)
    output = p.map(CF_Recommendation, m_imput)
    p.close()
    p.join()

    sys.exit()