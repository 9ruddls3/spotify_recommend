import multiprocessing


def get_num_cpus():
    maximum_cpus = int(multiprocessing.cpu_count())
    cpus = int(input("Set the number of Cpus (Maximum number of this devise : {}) => ".format(maximum_cpus)))

    while cpus > maximum_cpus:
        cpus = int(input("Set the number of Cpus (Maximum number of this devise : {}) => ".format(maximum_cpus)))

    return cpus


def multi_processing_setting(file_dir_list, num_cpu):
    if num_cpu == 1:
        unit_size = len(file_dir_list)
    else:
        unit_size = int(len(file_dir_list)//(num_cpu))

    multiprocess_list = []

    for x in range(num_cpu):
        start = x*unit_size
        end = (x+1)*unit_size
        if x == num_cpu-1:
            multiprocess_list.append(file_dir_list[start:])
        else:
            multiprocess_list.append(file_dir_list[start:end])

    return multiprocess_list