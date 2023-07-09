from utils import *

run_utils = RunUtils()

# Define jobs for all the tables and views belonging to different layers
layer0_jobs = ["nis_policies"]
layer1_jobs = []
layer2_jobs = []
layer3_jobs = []
all_views_jobs = []

# Running jobs for all layers such that jobs of each layer run in parallel
run_utils.run_parallel(layer0_jobs)
run_utils.run_parallel(layer1_jobs)
run_utils.run_parallel(layer2_jobs)
run_utils.run_parallel(layer3_jobs)
run_utils.run_parallel(all_views_jobs)
