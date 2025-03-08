import re
import matplotlib.pyplot as plt
import numpy as np
from collections import defaultdict

def parse_benchmarks_detailed(bench_str):
    """
    Parses benchmark lines extracting:
      - db_size: the number after "merkledb_"
      - key_size: the number in the "_<key>_keys_" part (values and keys are assumed equal)
      - op: the operation name (e.g., Delete, BatchWrite, ParallelGet, etc.)
      - iterations, ns/op, B/op, allocs/op
    Returns a dictionary keyed by (op, db_size, key_size).
    """
    # Modified regex: ns/op group now allows decimals (digits, commas, and a decimal point)
    pattern = re.compile(
        r"^Benchmark_MerkleDB_DBInterface/merkledb_(\d+)_1024_pairs_(\d+)_keys_(\d+)_values_([A-Za-z]+)-\d+\s+([\d,]+)\s+([\d.,]+)\s+ns\/op\s+([\d,]+)\s+B\/op\s+([\d,]+)\s+allocs\/op"
    )
    data = {}
    for line in bench_str.strip().splitlines():
        line = line.strip()
        m = pattern.match(line)
        if m:
            db_size = int(m.group(1))
            # pair_count = int(m.group(2))  # typically constant (1024_pairs)
            key_size = int(m.group(3))
            op = m.group(4)
            iterations = int(m.group(5).replace(',', ''))
            ns_op = float(m.group(6).replace(',', ''))  # Use float to accommodate decimals.
            b_op = int(m.group(7).replace(',', ''))
            allocs = int(m.group(8).replace(',', ''))
            data[(op, db_size, key_size)] = {
                'iterations': iterations,
                'ns_op': ns_op,
                'b_op': b_op,
                'allocs': allocs
            }
    return data

def compare_benchmarks_detailed(bench1, bench2):
    """
    For keys from the union of both benchmark sets (keyed by (op, db_size, key_size)),
    compute the percentage difference for each metric:
      percentage change = ((value2 - value1) / value1) * 100.
    If a key is missing in one of the sets, its value is assumed equal to the one present.
    Returns a dictionary with the same keys.
    """
    union_keys = set(bench1.keys()) | set(bench2.keys())
    diff = {}
    for key in union_keys:
        diff[key] = {}
        for metric in ['ns_op', 'b_op', 'allocs']:
            v1 = bench1[key][metric] if key in bench1 else None
            v2 = bench2[key][metric] if key in bench2 else None
            # If one value is missing, assume it to be equal to the other so that diff = 0.
            if v1 is None:
                v1 = v2
            if v2 is None:
                v2 = v1
            diff[key][metric] = ((v2 - v1) / v1 * 100) if v1 != 0 else 0
    return diff

def organize_diff_by_operation(diff):
    """
    Reorganizes the diff dictionary into a nested dict:
      diff_by_op[operation][key_size][db_size] = metrics diff.
    """
    diff_by_op = defaultdict(lambda: defaultdict(dict))
    for (op, db_size, key_size), metrics in diff.items():
        diff_by_op[op][key_size][db_size] = metrics
    return diff_by_op

def plot_diff_by_operation_grouped(diff_by_op):
    """
    For each operation, create a single figure with subplots for each key/value size.
    Each subplot shows a grouped bar chart (ns/op, B/op, allocs/op) for the different DB sizes.
    Up to three subplots are displayed per row.
    """
    # Fixed color mapping per metric: (color if negative, color if positive)
    color_map = {
        'ns_op': ('red', 'green'),
        'b_op': ('blue', 'orange'),
        'allocs': ('purple', 'cyan')
    }
    
    for op, keys_dict in diff_by_op.items():
        key_sizes = sorted(keys_dict.keys())
        n_keys = len(key_sizes)
        # Show up to 3 subplots per row.
        ncols = 3
        nrows = (n_keys + ncols - 1) // ncols  # ceiling division
        
        fig, axs = plt.subplots(nrows, ncols, figsize=(5*ncols, 5*nrows), squeeze=False)
        fig.suptitle(f"Operation: {op}", fontsize=16)
        
        for i, key_size in enumerate(key_sizes):
            row = i // ncols
            col = i % ncols
            ax = axs[row][col]
            db_dict = keys_dict[key_size]
            # Ensure the DB sizes are in sorted order.
            db_sizes = sorted(db_dict.keys())
            ns_values = [db_dict[db]['ns_op'] for db in db_sizes]
            b_values = [db_dict[db]['b_op'] for db in db_sizes]
            allocs_values = [db_dict[db]['allocs'] for db in db_sizes]
            
            x = np.arange(len(db_sizes))
            width = 0.25
            
            # Create bars with fixed colors per metric.
            bars_ns = ax.bar(
                x - width, ns_values, width, label='ns/op',
                color=[color_map['ns_op'][0] if v < 0 else color_map['ns_op'][1] for v in ns_values]
            )
            bars_b = ax.bar(
                x, b_values, width, label='B/op',
                color=[color_map['b_op'][0] if v < 0 else color_map['b_op'][1] for v in b_values]
            )
            bars_allocs = ax.bar(
                x + width, allocs_values, width, label='allocs/op',
                color=[color_map['allocs'][0] if v < 0 else color_map['allocs'][1] for v in allocs_values]
            )
            
            ax.set_xlabel('DB size (merkledb_X)')
            ax.set_xticks(x)
            ax.set_xticklabels([str(db) for db in db_sizes])
            ax.set_ylabel('Percentage change (%)')
            ax.set_title(f"Key/Value size: {key_size}")
            ax.axhline(0, color='black', linewidth=0.8)
            ax.legend(fontsize=8)
            
            # Add text labels above each bar.
            for j, v in enumerate(ns_values):
                ax.text(x[j] - width, v, f"{v:.1f}%", ha='center', va='bottom', fontsize=8)
            for j, v in enumerate(b_values):
                ax.text(x[j], v, f"{v:.1f}%", ha='center', va='bottom', fontsize=8)
            for j, v in enumerate(allocs_values):
                ax.text(x[j] + width, v, f"{v:.1f}%", ha='center', va='bottom', fontsize=8)
        
        # Hide any unused subplots.
        total_subplots = nrows * ncols
        if total_subplots > n_keys:
            for i in range(n_keys, total_subplots):
                row = i // ncols
                col = i % ncols
                axs[row][col].axis('off')
                
        plt.tight_layout(rect=[0, 0, 1, 0.95])
        plt.show()


if __name__ == "__main__":
    # --- Sample input strings ---
    # Replace these strings with your actual benchmark outputs.
    bench_str1 = """
Benchmark_MerkleDB_DBInterface/merkledb_2_1024_pairs_32_keys_32_values_Delete-12         	    8067	    126044 ns/op	   11522 B/op	     156 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_2_1024_pairs_32_keys_32_values_BatchWrite-12     	     100	  79435234 ns/op	13014340 B/op	  190448 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_2_1024_pairs_32_keys_32_values_ParallelGet-12    	 1624815	       720.7 ns/op	     416 B/op	      10 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_2_1024_pairs_32_keys_32_values_ParallelPut-12    	    2240	    682686 ns/op	   44412 B/op	     586 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_2_1024_pairs_32_keys_32_values_ParallelDelete-12 	   37150	     27209 ns/op	    5902 B/op	      70 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_2_1024_pairs_32_keys_32_values_Get-12            	 1533818	       724.0 ns/op	     415 B/op	      10 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_2_1024_pairs_32_keys_32_values_Put-12            	    1723	    825558 ns/op	   72331 B/op	     962 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_4_1024_pairs_32_keys_32_values_ParallelDelete-12 	  103514	     10456 ns/op	    4577 B/op	      50 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_4_1024_pairs_32_keys_32_values_Get-12            	 1608061	       739.6 ns/op	     415 B/op	      10 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_4_1024_pairs_32_keys_32_values_Put-12            	    2275	    522840 ns/op	   46992 B/op	     538 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_4_1024_pairs_32_keys_32_values_Delete-12         	  135913	      7768 ns/op	    4448 B/op	      48 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_4_1024_pairs_32_keys_32_values_BatchWrite-12     	     100	  34994273 ns/op	 8271106 B/op	  113269 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_4_1024_pairs_32_keys_32_values_ParallelGet-12    	 2042518	       585.5 ns/op	     415 B/op	      10 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_4_1024_pairs_32_keys_32_values_ParallelPut-12    	    2047	    514857 ns/op	   31802 B/op	     343 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_16_1024_pairs_32_keys_32_values_Put-12           	    2476	    462139 ns/op	   53235 B/op	     467 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_16_1024_pairs_32_keys_32_values_Delete-12        	  129934	      9526 ns/op	    4495 B/op	      48 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_16_1024_pairs_32_keys_32_values_BatchWrite-12    	     100	  20901970 ns/op	 7743596 B/op	   84909 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_16_1024_pairs_32_keys_32_values_ParallelGet-12   	 1663791	       762.9 ns/op	     415 B/op	      10 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_16_1024_pairs_32_keys_32_values_ParallelPut-12   	    2132	    548670 ns/op	   32511 B/op	     291 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_16_1024_pairs_32_keys_32_values_ParallelDelete-12         	  137877	      7632 ns/op	    4581 B/op	      48 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_16_1024_pairs_32_keys_32_values_Get-12                    	 1762561	       732.3 ns/op	     415 B/op	      10 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_256_1024_pairs_32_keys_32_values_ParallelGet-12           	 2118614	       566.1 ns/op	     415 B/op	      10 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_256_1024_pairs_32_keys_32_values_ParallelPut-12           	    2053	    622703 ns/op	   64073 B/op	     532 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_256_1024_pairs_32_keys_32_values_ParallelDelete-12        	    7159	    149580 ns/op	   66728 B/op	     619 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_256_1024_pairs_32_keys_32_values_Get-12                   	 1665997	       767.1 ns/op	     415 B/op	      10 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_256_1024_pairs_32_keys_32_values_Put-12                   	    1489	    727007 ns/op	  203068 B/op	    1599 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_256_1024_pairs_32_keys_32_values_Delete-12                	  116457	      9066 ns/op	    5611 B/op	      56 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_256_1024_pairs_32_keys_32_values_BatchWrite-12            	     100	  21853933 ns/op	22775362 B/op	  178299 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_2_1024_pairs_256_keys_256_values_ParallelPut-12           	    2443	    676658 ns/op	   54396 B/op	     607 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_2_1024_pairs_256_keys_256_values_ParallelDelete-12        	   22845	     49186 ns/op	    8209 B/op	      87 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_2_1024_pairs_256_keys_256_values_Get-12                   	  894346	      1275 ns/op	    1103 B/op	      10 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_2_1024_pairs_256_keys_256_values_Put-12                   	    1520	    993861 ns/op	   79724 B/op	     931 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_2_1024_pairs_256_keys_256_values_Delete-12                	    1116	    909631 ns/op	   63776 B/op	     857 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_2_1024_pairs_256_keys_256_values_BatchWrite-12            	      70	  95051614 ns/op	18792271 B/op	  189410 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_2_1024_pairs_256_keys_256_values_ParallelGet-12           	 1390196	       847.0 ns/op	    1103 B/op	      10 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_4_1024_pairs_256_keys_256_values_Get-12                   	 1115325	      1122 ns/op	    1103 B/op	      10 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_4_1024_pairs_256_keys_256_values_Put-12                   	    2325	    554259 ns/op	   57293 B/op	     536 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_4_1024_pairs_256_keys_256_values_Delete-12                	   52034	     19543 ns/op	    5972 B/op	      54 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_4_1024_pairs_256_keys_256_values_BatchWrite-12            	      98	  46989593 ns/op	14433638 B/op	  112904 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_4_1024_pairs_256_keys_256_values_ParallelGet-12           	 2159552	       594.2 ns/op	    1103 B/op	      10 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_4_1024_pairs_256_keys_256_values_ParallelPut-12           	    2184	    470487 ns/op	   42474 B/op	     354 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_4_1024_pairs_256_keys_256_values_ParallelDelete-12        	  131427	      8285 ns/op	    5516 B/op	      49 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_16_1024_pairs_256_keys_256_values_ParallelDelete-12       	  116683	     10067 ns/op	    5769 B/op	      49 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_16_1024_pairs_256_keys_256_values_Get-12                  	 1276891	       884.4 ns/op	    1103 B/op	      10 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_16_1024_pairs_256_keys_256_values_Put-12                  	    2078	    537454 ns/op	   71707 B/op	     460 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_16_1024_pairs_256_keys_256_values_Delete-12               	  181916	      6553 ns/op	    5461 B/op	      47 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_16_1024_pairs_256_keys_256_values_BatchWrite-12           	     100	  24401280 ns/op	14224390 B/op	   85376 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_16_1024_pairs_256_keys_256_values_ParallelGet-12          	 2141138	       655.6 ns/op	    1103 B/op	      10 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_16_1024_pairs_256_keys_256_values_ParallelPut-12          	    2415	    461099 ns/op	   51290 B/op	     309 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_256_1024_pairs_256_keys_256_values_Get-12                 	 1403350	       827.5 ns/op	    1103 B/op	      10 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_256_1024_pairs_256_keys_256_values_Put-12                 	    2300	    645404 ns/op	  291949 B/op	    1658 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_256_1024_pairs_256_keys_256_values_Delete-12              	  103884	      9755 ns/op	    7822 B/op	      58 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_256_1024_pairs_256_keys_256_values_BatchWrite-12          	     100	  32477850 ns/op	34297666 B/op	  185445 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_256_1024_pairs_256_keys_256_values_ParallelGet-12         	 1772977	       586.1 ns/op	    1103 B/op	      10 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_256_1024_pairs_256_keys_256_values_ParallelPut-12         	    1909	    649502 ns/op	  138098 B/op	     528 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_256_1024_pairs_256_keys_256_values_ParallelDelete-12      	    8389	    137420 ns/op	   75929 B/op	     590 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_2_1024_pairs_2048_keys_2048_values_ParallelDelete-12      	   19838	     51548 ns/op	   18984 B/op	      93 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_2_1024_pairs_2048_keys_2048_values_Get-12                 	  633679	      3288 ns/op	    6703 B/op	      10 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_2_1024_pairs_2048_keys_2048_values_Put-12                 	     938	   1083099 ns/op	  140481 B/op	     818 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_2_1024_pairs_2048_keys_2048_values_Delete-12              	   34564	     29568 ns/op	   16398 B/op	      71 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_2_1024_pairs_2048_keys_2048_values_BatchWrite-12          	      37	 168569032 ns/op	66318469 B/op	  187871 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_2_1024_pairs_2048_keys_2048_values_ParallelGet-12         	 1344744	       786.2 ns/op	    6703 B/op	      10 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_2_1024_pairs_2048_keys_2048_values_ParallelPut-12         	    1718	    679750 ns/op	  121669 B/op	     535 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_4_1024_pairs_2048_keys_2048_values_Get-12                 	  546355	      1978 ns/op	    6703 B/op	      10 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_4_1024_pairs_2048_keys_2048_values_Put-12                 	    1456	    770109 ns/op	  136494 B/op	     501 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_4_1024_pairs_2048_keys_2048_values_Delete-12              	  100112	     10556 ns/op	   14217 B/op	      49 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_4_1024_pairs_2048_keys_2048_values_BatchWrite-12          	      46	  89121948 ns/op	62253886 B/op	  111942 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_4_1024_pairs_2048_keys_2048_values_ParallelGet-12         	 1452489	       762.4 ns/op	    6703 B/op	      10 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_4_1024_pairs_2048_keys_2048_values_ParallelPut-12         	    2368	    524796 ns/op	  123198 B/op	     352 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_4_1024_pairs_2048_keys_2048_values_ParallelDelete-12      	   28863	     37930 ns/op	   16923 B/op	      64 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_16_1024_pairs_2048_keys_2048_values_Put-12                	    2098	    607418 ns/op	  201174 B/op	     457 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_16_1024_pairs_2048_keys_2048_values_Delete-12             	  118306	      9531 ns/op	   14704 B/op	      48 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_16_1024_pairs_2048_keys_2048_values_BatchWrite-12         	      49	  53954180 ns/op	62030841 B/op	   84017 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_16_1024_pairs_2048_keys_2048_values_ParallelGet-12        	 1401019	       729.4 ns/op	    6704 B/op	      10 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_16_1024_pairs_2048_keys_2048_values_ParallelPut-12        	    2164	    490429 ns/op	  166730 B/op	     294 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_16_1024_pairs_2048_keys_2048_values_ParallelDelete-12     	   71875	     14643 ns/op	   16158 B/op	      55 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_16_1024_pairs_2048_keys_2048_values_Get-12                	  638206	      2353 ns/op	    6704 B/op	      10 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_256_1024_pairs_2048_keys_2048_values_ParallelGet-12       	 1376769	      1202 ns/op	    6703 B/op	      10 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_256_1024_pairs_2048_keys_2048_values_ParallelPut-12       	    1777	   1095206 ns/op	  626614 B/op	     479 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_256_1024_pairs_2048_keys_2048_values_ParallelDelete-12    	    7081	    184202 ns/op	  180245 B/op	     627 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_256_1024_pairs_2048_keys_2048_values_Get-12               	  594469	      2907 ns/op	    6703 B/op	      10 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_256_1024_pairs_2048_keys_2048_values_Put-12               	    1898	    794022 ns/op	  913173 B/op	    1614 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_256_1024_pairs_2048_keys_2048_values_Delete-12            	    3867	    401730 ns/op	  282690 B/op	     402 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_256_1024_pairs_2048_keys_2048_values_BatchWrite-12        	      32	  62429472 ns/op	103188607 B/op	  175385 allocs/op
"""
    # A second benchmark string; here we simulate some differences by tweaking the numbers.
    bench_str2 = """
Benchmark_MerkleDB_DBInterface/merkledb_2_1024_pairs_32_keys_32_values_Delete-12                  606022              1919 ns/op            3450 B/op         25 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_2_1024_pairs_32_keys_32_values_BatchWrite-12                 135           8711351 ns/op         3197731 B/op      31343 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_2_1024_pairs_32_keys_32_values_ParallelGet-12            2167849               559.4 ns/op           208 B/op          5 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_2_1024_pairs_32_keys_32_values_ParallelPut-12              23853             53867 ns/op           22321 B/op        220 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_2_1024_pairs_32_keys_32_values_ParallelDelete-12          276682              3835 ns/op            3484 B/op         25 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_2_1024_pairs_32_keys_32_values_Get-12                    2020380               548.0 ns/op           208 B/op          5 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_2_1024_pairs_32_keys_32_values_Put-12                      17073             68154 ns/op           23450 B/op        222 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_4_1024_pairs_32_keys_32_values_Put-12                      26124             47576 ns/op           17612 B/op        166 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_4_1024_pairs_32_keys_32_values_Delete-12                  541275              2180 ns/op            3446 B/op         25 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_4_1024_pairs_32_keys_32_values_BatchWrite-12                 158           9549935 ns/op         2473991 B/op      22162 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_4_1024_pairs_32_keys_32_values_ParallelGet-12            1468522              1118 ns/op             208 B/op          5 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_4_1024_pairs_32_keys_32_values_ParallelPut-12              22179             56126 ns/op           17671 B/op        164 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_4_1024_pairs_32_keys_32_values_ParallelDelete-12          256494              4090 ns/op            3473 B/op         25 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_4_1024_pairs_32_keys_32_values_Get-12                    2227004               557.0 ns/op           208 B/op          5 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_16_1024_pairs_32_keys_32_values_ParallelGet-12           1541641               779.2 ns/op           208 B/op          5 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_16_1024_pairs_32_keys_32_values_ParallelPut-12             21726             60658 ns/op           22165 B/op        181 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_16_1024_pairs_32_keys_32_values_ParallelDelete-12                 259716              3920 ns/op            3494 B/op         25 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_16_1024_pairs_32_keys_32_values_Get-12                           2310726               545.8 ns/op           208 B/op          5 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_16_1024_pairs_32_keys_32_values_Put-12                             24762             52335 ns/op           22161 B/op        182 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_16_1024_pairs_32_keys_32_values_Delete-12                         597252              2012 ns/op            3450 B/op         25 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_16_1024_pairs_32_keys_32_values_BatchWrite-12                        285           4444315 ns/op         2026786 B/op      16166 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_256_1024_pairs_32_keys_32_values_ParallelPut-12                    10000            152811 ns/op           95557 B/op        712 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_256_1024_pairs_32_keys_32_values_ParallelDelete-12                118893              9197 ns/op            6918 B/op         56 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_256_1024_pairs_32_keys_32_values_Get-12                          2332216               522.9 ns/op           208 B/op          5 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_256_1024_pairs_32_keys_32_values_Put-12                            10000            174139 ns/op          107905 B/op        826 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_256_1024_pairs_32_keys_32_values_Delete-12                        540813              2041 ns/op            3591 B/op         26 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_256_1024_pairs_32_keys_32_values_BatchWrite-12                       349           3377677 ns/op         1882185 B/op      14104 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_256_1024_pairs_32_keys_32_values_ParallelGet-12                  2211526               552.3 ns/op           208 B/op          5 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_2_1024_pairs_256_keys_256_values_ParallelPut-12                    18416             65799 ns/op           26024 B/op        227 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_2_1024_pairs_256_keys_256_values_ParallelDelete-12                277788              5135 ns/op            3492 B/op         25 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_2_1024_pairs_256_keys_256_values_Get-12                          1918507               655.4 ns/op           656 B/op          5 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_2_1024_pairs_256_keys_256_values_Put-12                            15189             81602 ns/op           26630 B/op        228 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_2_1024_pairs_256_keys_256_values_Delete-12                        490267              2383 ns/op            3461 B/op         25 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_2_1024_pairs_256_keys_256_values_BatchWrite-12                       100          19714630 ns/op         5935083 B/op      33060 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_2_1024_pairs_256_keys_256_values_ParallelGet-12                  1554502               779.9 ns/op           656 B/op          5 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_4_1024_pairs_256_keys_256_values_Get-12                          1872217               666.7 ns/op           656 B/op          5 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_4_1024_pairs_256_keys_256_values_Put-12                            21265             56643 ns/op           21482 B/op        171 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_4_1024_pairs_256_keys_256_values_Delete-12                        547382              2316 ns/op            3450 B/op         25 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_4_1024_pairs_256_keys_256_values_BatchWrite-12                       100          12078021 ns/op         5334783 B/op      23765 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_4_1024_pairs_256_keys_256_values_ParallelGet-12                  1553620               767.8 ns/op           656 B/op          5 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_4_1024_pairs_256_keys_256_values_ParallelPut-12                    19024             65361 ns/op           21598 B/op        171 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_4_1024_pairs_256_keys_256_values_ParallelDelete-12                281832              5081 ns/op            3478 B/op         25 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_16_1024_pairs_256_keys_256_values_Get-12                         1826692               698.5 ns/op           656 B/op          5 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_16_1024_pairs_256_keys_256_values_Put-12                           19698             60110 ns/op           26518 B/op        187 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_16_1024_pairs_256_keys_256_values_Delete-12                       474770              2314 ns/op            3464 B/op         25 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_16_1024_pairs_256_keys_256_values_BatchWrite-12                      160           7347888 ns/op         4835370 B/op      17519 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_16_1024_pairs_256_keys_256_values_ParallelGet-12                 2162096               562.0 ns/op           656 B/op          5 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_16_1024_pairs_256_keys_256_values_ParallelPut-12                   23643             51271 ns/op           26179 B/op        186 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_16_1024_pairs_256_keys_256_values_ParallelDelete-12               442194              2964 ns/op            3471 B/op         25 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_256_1024_pairs_256_keys_256_values_ParallelDelete-12              102321              9826 ns/op            7898 B/op         63 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_256_1024_pairs_256_keys_256_values_Get-12                        1953824               712.6 ns/op           656 B/op          5 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_256_1024_pairs_256_keys_256_values_Put-12                          10000            167600 ns/op          128312 B/op        842 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_256_1024_pairs_256_keys_256_values_Delete-12                      391153              2776 ns/op            3738 B/op         26 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_256_1024_pairs_256_keys_256_values_BatchWrite-12                     184           6414198 ns/op         4670607 B/op      15333 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_256_1024_pairs_256_keys_256_values_ParallelGet-12                1550438               775.1 ns/op           656 B/op          5 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_256_1024_pairs_256_keys_256_values_ParallelPut-12                  10000            169952 ns/op          131328 B/op        711 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_2_1024_pairs_2048_keys_2048_values_Get-12                         201868              6690 ns/op           12167 B/op         17 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_2_1024_pairs_2048_keys_2048_values_Put-12                           9700            236459 ns/op          117751 B/op        378 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_2_1024_pairs_2048_keys_2048_values_Delete-12                      110016             11064 ns/op           15718 B/op         46 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_2_1024_pairs_2048_keys_2048_values_BatchWrite-12                      51         141944308 ns/op        64781177 B/op      95989 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_2_1024_pairs_2048_keys_2048_values_ParallelGet-12                1155688              1003 ns/op           12088 B/op         15 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_2_1024_pairs_2048_keys_2048_values_ParallelPut-12                   7076            177573 ns/op           60424 B/op        207 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_2_1024_pairs_2048_keys_2048_values_ParallelDelete-12               66601             15638 ns/op           15702 B/op         47 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_4_1024_pairs_2048_keys_2048_values_Get-12                         208437              6907 ns/op           11930 B/op         16 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_4_1024_pairs_2048_keys_2048_values_Put-12                           9548            190783 ns/op          120089 B/op        310 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_4_1024_pairs_2048_keys_2048_values_Delete-12                      182011              5916 ns/op            9943 B/op         36 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_4_1024_pairs_2048_keys_2048_values_BatchWrite-12                      60          76759443 ns/op        63336358 B/op      73185 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_4_1024_pairs_2048_keys_2048_values_ParallelGet-12                1249963               928.8 ns/op         11864 B/op         15 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_4_1024_pairs_2048_keys_2048_values_ParallelPut-12                  11324            109749 ns/op           62461 B/op        185 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_4_1024_pairs_2048_keys_2048_values_ParallelDelete-12              183841              6576 ns/op            9559 B/op         35 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_16_1024_pairs_2048_keys_2048_values_Put-12                         10000            144545 ns/op          139921 B/op        318 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_16_1024_pairs_2048_keys_2048_values_Delete-12                     148218              6828 ns/op           10484 B/op         41 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_16_1024_pairs_2048_keys_2048_values_BatchWrite-12                     79          49527325 ns/op        63238078 B/op      57649 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_16_1024_pairs_2048_keys_2048_values_ParallelGet-12               1064394              1126 ns/op           13621 B/op         19 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_16_1024_pairs_2048_keys_2048_values_ParallelPut-12                 12357            104600 ns/op           86595 B/op        207 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_16_1024_pairs_2048_keys_2048_values_ParallelDelete-12             119232              8636 ns/op           11052 B/op         42 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_16_1024_pairs_2048_keys_2048_values_Get-12                        181191              6251 ns/op           13680 B/op         20 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_256_1024_pairs_2048_keys_2048_values_ParallelPut-12                10000            303806 ns/op          437555 B/op        697 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_256_1024_pairs_2048_keys_2048_values_ParallelDelete-12             35574             30576 ns/op           33505 B/op        175 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_256_1024_pairs_2048_keys_2048_values_Get-12                       179374              7063 ns/op           13321 B/op         21 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_256_1024_pairs_2048_keys_2048_values_Put-12                         7713            275772 ns/op          336306 B/op        987 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_256_1024_pairs_2048_keys_2048_values_Delete-12                     95526             11573 ns/op           16093 B/op         58 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_256_1024_pairs_2048_keys_2048_values_BatchWrite-12                    92          37342318 ns/op        62074654 B/op      50402 allocs/op
Benchmark_MerkleDB_DBInterface/merkledb_256_1024_pairs_2048_keys_2048_values_ParallelGet-12              1028581              1111 ns/op           13255 B/op         20 allocs/op                 91224             11946 ns/op           16324 B/op         58 allocs/op	    910558 ns/op	   80031 B/op	    1050 allocs/op
"""
    # Parse benchmark strings.
    benchmarks1 = parse_benchmarks_detailed(bench_str1)
    benchmarks2 = parse_benchmarks_detailed(bench_str2)
    
    # Compute percentage differences for common benchmarks.
    diff = compare_benchmarks_detailed(benchmarks1, benchmarks2)
    
    # Organize the differences by operation.
    diff_by_op = organize_diff_by_operation(diff)
    
    # Plot one figure per operation, with subplots for each key/value size.
    plot_diff_by_operation_grouped(diff_by_op)