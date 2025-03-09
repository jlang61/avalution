// Copyright (C) 2019-2024, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package dbtest

import (
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ava-labs/avalanchego/database"
	"github.com/ava-labs/avalanchego/utils/units"
)

/*
                                                                                      │ diffLayerComplete.txt │           regularComplete.txt           │
                                                                                      │        sec/op         │     sec/op      vs base                 │
_MerkleDB_DBInterface/merkledb_2_1024_pairs_32_keys_32_values_Get-10                             652.3n ± ∞ ¹     694.4n ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_2_1024_pairs_32_keys_32_values_Put-10                             46.87µ ± ∞ ¹     70.51µ ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_2_1024_pairs_32_keys_32_values_BatchDelete-10                     272.5n ± ∞ ¹     279.6n ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_2_1024_pairs_32_keys_32_values_ParallelPut-10                     45.81µ ± ∞ ¹     69.52µ ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_2_1024_pairs_32_keys_32_values_Delete-10                          2.448µ ± ∞ ¹     2.801µ ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_2_1024_pairs_32_keys_32_values_BatchPut-10                        315.7n ± ∞ ¹     293.9n ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_2_1024_pairs_32_keys_32_values_BatchWrite-10                      6.099m ± ∞ ¹    10.150m ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_2_1024_pairs_32_keys_32_values_ParallelGet-10                     1.207µ ± ∞ ¹     1.236µ ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_2_1024_pairs_32_keys_32_values_ParallelDelete-10                  2.819µ ± ∞ ¹     3.086µ ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_4_1024_pairs_32_keys_32_values_Delete-10                          2.394µ ± ∞ ¹     2.722µ ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_4_1024_pairs_32_keys_32_values_BatchPut-10                        314.0n ± ∞ ¹     273.9n ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_4_1024_pairs_32_keys_32_values_BatchWrite-10                      4.144m ± ∞ ¹     7.253m ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_4_1024_pairs_32_keys_32_values_ParallelGet-10                     1.238µ ± ∞ ¹     1.292µ ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_4_1024_pairs_32_keys_32_values_ParallelDelete-10                  3.045µ ± ∞ ¹     3.249µ ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_4_1024_pairs_32_keys_32_values_Get-10                             654.0n ± ∞ ¹     690.4n ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_4_1024_pairs_32_keys_32_values_Put-10                             29.94µ ± ∞ ¹     51.81µ ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_4_1024_pairs_32_keys_32_values_BatchDelete-10                     280.3n ± ∞ ¹     285.8n ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_4_1024_pairs_32_keys_32_values_ParallelPut-10                     31.66µ ± ∞ ¹     51.87µ ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_16_1024_pairs_32_keys_32_values_Get-10                            647.1n ± ∞ ¹     736.7n ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_16_1024_pairs_32_keys_32_values_Put-10                            30.88µ ± ∞ ¹     61.87µ ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_16_1024_pairs_32_keys_32_values_BatchDelete-10                    291.6n ± ∞ ¹     231.5n ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_16_1024_pairs_32_keys_32_values_ParallelPut-10                    33.11µ ± ∞ ¹     55.79µ ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_16_1024_pairs_32_keys_32_values_ParallelDelete-10                 3.370µ ± ∞ ¹     3.089µ ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_16_1024_pairs_32_keys_32_values_Delete-10                         2.474µ ± ∞ ¹     2.535µ ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_16_1024_pairs_32_keys_32_values_BatchPut-10                       286.8n ± ∞ ¹     274.8n ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_16_1024_pairs_32_keys_32_values_BatchWrite-10                     2.886m ± ∞ ¹     5.523m ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_16_1024_pairs_32_keys_32_values_ParallelGet-10                    1.210µ ± ∞ ¹     1.242µ ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_256_1024_pairs_32_keys_32_values_Delete-10                        3.029µ ± ∞ ¹     2.982µ ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_256_1024_pairs_32_keys_32_values_BatchPut-10                      297.9n ± ∞ ¹     283.6n ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_256_1024_pairs_32_keys_32_values_BatchWrite-10                    2.307m ± ∞ ¹     4.702m ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_256_1024_pairs_32_keys_32_values_ParallelGet-10                   1.195µ ± ∞ ¹     1.241µ ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_256_1024_pairs_32_keys_32_values_ParallelDelete-10                3.640µ ± ∞ ¹     4.355µ ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_256_1024_pairs_32_keys_32_values_Get-10                           648.8n ± ∞ ¹     701.4n ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_256_1024_pairs_32_keys_32_values_Put-10                           128.0µ ± ∞ ¹     218.2µ ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_256_1024_pairs_32_keys_32_values_BatchDelete-10                   291.6n ± ∞ ¹     230.7n ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_256_1024_pairs_32_keys_32_values_ParallelPut-10                   121.8µ ± ∞ ¹     208.2µ ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_2_1024_pairs_256_keys_256_values_BatchDelete-10                   293.5n ± ∞ ¹     264.3n ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_2_1024_pairs_256_keys_256_values_ParallelPut-10                   61.46µ ± ∞ ¹     86.48µ ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_2_1024_pairs_256_keys_256_values_Get-10                           725.8n ± ∞ ¹     776.4n ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_2_1024_pairs_256_keys_256_values_Put-10                           57.68µ ± ∞ ¹     97.58µ ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_2_1024_pairs_256_keys_256_values_BatchWrite-10                    17.79m ± ∞ ¹     24.06m ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_2_1024_pairs_256_keys_256_values_ParallelGet-10                   1.288µ ± ∞ ¹     1.303µ ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_2_1024_pairs_256_keys_256_values_ParallelDelete-10                3.643µ ± ∞ ¹     3.371µ ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_2_1024_pairs_256_keys_256_values_Delete-10                        2.710µ ± ∞ ¹     3.037µ ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_2_1024_pairs_256_keys_256_values_BatchPut-10                      433.1n ± ∞ ¹     335.4n ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_4_1024_pairs_256_keys_256_values_Get-10                           772.7n ± ∞ ¹     777.2n ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_4_1024_pairs_256_keys_256_values_Put-10                           38.73µ ± ∞ ¹     69.59µ ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_4_1024_pairs_256_keys_256_values_BatchDelete-10                   304.2n ± ∞ ¹     265.2n ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_4_1024_pairs_256_keys_256_values_ParallelPut-10                   38.60µ ± ∞ ¹     70.59µ ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_4_1024_pairs_256_keys_256_values_Delete-10                        2.655µ ± ∞ ¹     2.801µ ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_4_1024_pairs_256_keys_256_values_BatchPut-10                      467.2n ± ∞ ¹     347.8n ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_4_1024_pairs_256_keys_256_values_BatchWrite-10                    10.04m ± ∞ ¹     16.43m ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_4_1024_pairs_256_keys_256_values_ParallelGet-10                   1.294µ ± ∞ ¹     1.295µ ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_4_1024_pairs_256_keys_256_values_ParallelDelete-10                3.971µ ± ∞ ¹     3.254µ ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_16_1024_pairs_256_keys_256_values_ParallelGet-10                  1.395µ ± ∞ ¹     1.320µ ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_16_1024_pairs_256_keys_256_values_ParallelDelete-10               3.287µ ± ∞ ¹     3.328µ ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_16_1024_pairs_256_keys_256_values_Delete-10                       2.639µ ± ∞ ¹     2.844µ ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_16_1024_pairs_256_keys_256_values_BatchPut-10                     506.0n ± ∞ ¹     302.9n ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_16_1024_pairs_256_keys_256_values_BatchWrite-10                   5.826m ± ∞ ¹    11.662m ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_16_1024_pairs_256_keys_256_values_ParallelPut-10                  35.67µ ± ∞ ¹     78.77µ ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_16_1024_pairs_256_keys_256_values_Get-10                          728.9n ± ∞ ¹     735.5n ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_16_1024_pairs_256_keys_256_values_Put-10                          35.66µ ± ∞ ¹     76.60µ ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_16_1024_pairs_256_keys_256_values_BatchDelete-10                  257.9n ± ∞ ¹     264.9n ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_256_1024_pairs_256_keys_256_values_Get-10                         707.1n ± ∞ ¹     828.0n ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_256_1024_pairs_256_keys_256_values_Put-10                         125.4µ ± ∞ ¹     261.6µ ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_256_1024_pairs_256_keys_256_values_BatchDelete-10                 268.0n ± ∞ ¹     286.5n ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_256_1024_pairs_256_keys_256_values_ParallelPut-10                 124.1µ ± ∞ ¹     255.6µ ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_256_1024_pairs_256_keys_256_values_Delete-10                      2.893µ ± ∞ ¹     3.215µ ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_256_1024_pairs_256_keys_256_values_BatchPut-10                    353.4n ± ∞ ¹     382.7n ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_256_1024_pairs_256_keys_256_values_BatchWrite-10                  4.024m ± ∞ ¹    10.215m ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_256_1024_pairs_256_keys_256_values_ParallelGet-10                 1.257µ ± ∞ ¹     1.304µ ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_256_1024_pairs_256_keys_256_values_ParallelDelete-10              3.756µ ± ∞ ¹     5.734µ ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_2_1024_pairs_2048_keys_2048_values_Get-10                         2.082µ ± ∞ ¹     6.511µ ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_2_1024_pairs_2048_keys_2048_values_Put-10                         143.8µ ± ∞ ¹     323.0µ ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_2_1024_pairs_2048_keys_2048_values_BatchDelete-10                1019.0n ± ∞ ¹     886.4n ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_2_1024_pairs_2048_keys_2048_values_ParallelPut-10                 139.4µ ± ∞ ¹     194.6µ ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_2_1024_pairs_2048_keys_2048_values_Delete-10                      3.268µ ± ∞ ¹    13.267µ ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_2_1024_pairs_2048_keys_2048_values_BatchPut-10                    1.449µ ± ∞ ¹     1.759µ ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_2_1024_pairs_2048_keys_2048_values_BatchWrite-10                  144.8m ± ∞ ¹     135.5m ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_2_1024_pairs_2048_keys_2048_values_ParallelGet-10                 1.868µ ± ∞ ¹     2.339µ ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_2_1024_pairs_2048_keys_2048_values_ParallelDelete-10              3.789µ ± ∞ ¹    13.463µ ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_4_1024_pairs_2048_keys_2048_values_Delete-10                      3.032µ ± ∞ ¹     7.287µ ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_4_1024_pairs_2048_keys_2048_values_BatchPut-10                   1288.0n ± ∞ ¹     809.2n ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_4_1024_pairs_2048_keys_2048_values_BatchWrite-10                  58.60m ± ∞ ¹     83.46m ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_4_1024_pairs_2048_keys_2048_values_ParallelGet-10                 1.643µ ± ∞ ¹     2.415µ ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_4_1024_pairs_2048_keys_2048_values_ParallelDelete-10              3.705µ ± ∞ ¹     8.684µ ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_4_1024_pairs_2048_keys_2048_values_Get-10                         1.345µ ± ∞ ¹     6.245µ ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_4_1024_pairs_2048_keys_2048_values_Put-10                         89.14µ ± ∞ ¹    210.18µ ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_4_1024_pairs_2048_keys_2048_values_BatchDelete-10                 676.8n ± ∞ ¹     572.5n ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_4_1024_pairs_2048_keys_2048_values_ParallelPut-10                 94.75µ ± ∞ ¹    160.70µ ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_16_1024_pairs_2048_keys_2048_values_Get-10                        1.539µ ± ∞ ¹     8.778µ ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_16_1024_pairs_2048_keys_2048_values_Put-10                        71.64µ ± ∞ ¹    240.30µ ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_16_1024_pairs_2048_keys_2048_values_BatchDelete-10                714.1n ± ∞ ¹     534.6n ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_16_1024_pairs_2048_keys_2048_values_ParallelPut-10                67.53µ ± ∞ ¹    178.61µ ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_16_1024_pairs_2048_keys_2048_values_ParallelDelete-10             3.432µ ± ∞ ¹    12.860µ ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_16_1024_pairs_2048_keys_2048_values_Delete-10                     3.043µ ± ∞ ¹     9.891µ ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_16_1024_pairs_2048_keys_2048_values_BatchPut-10                   1.434µ ± ∞ ¹     1.529µ ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_16_1024_pairs_2048_keys_2048_values_BatchWrite-10                 34.57m ± ∞ ¹     65.88m ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_16_1024_pairs_2048_keys_2048_values_ParallelGet-10                1.723µ ± ∞ ¹     2.884µ ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_256_1024_pairs_2048_keys_2048_values_BatchWrite-10                17.84m ± ∞ ¹     59.57m ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_256_1024_pairs_2048_keys_2048_values_ParallelGet-10               1.567µ ± ∞ ¹     3.328µ ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_256_1024_pairs_2048_keys_2048_values_ParallelDelete-10            4.576µ ± ∞ ¹   141.318µ ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_256_1024_pairs_2048_keys_2048_values_Delete-10                    3.424µ ± ∞ ¹    49.997µ ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_256_1024_pairs_2048_keys_2048_values_BatchPut-10                  1.655µ ± ∞ ¹     1.711µ ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_256_1024_pairs_2048_keys_2048_values_BatchDelete-10              2076.0n ± ∞ ¹     674.1n ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_256_1024_pairs_2048_keys_2048_values_ParallelPut-10               130.6µ ± ∞ ¹     823.3µ ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_256_1024_pairs_2048_keys_2048_values_Get-10                       1.619µ ± ∞ ¹    11.353µ ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_256_1024_pairs_2048_keys_2048_values_Put-10                       134.7µ ± ∞ ¹     701.5µ ± ∞ ¹        ~ (p=1.000 n=1) ²
geomean                                                                                          8.131µ           12.18µ        +49.79%
*/
var (
	// Benchmarks is a list of all database benchmarks
	Benchmarks = map[string]func(b *testing.B, db database.Database, keys, values [][]byte){
		"Get":            BenchmarkGet,
		"Put":            BenchmarkPut,
		"Delete":         BenchmarkDelete,
		"BatchPut":       BenchmarkBatchPut,
		"BatchDelete":    BenchmarkBatchDelete,
		"BatchWrite":     BenchmarkBatchWrite,
		"ParallelGet":    BenchmarkParallelGet,
		"ParallelPut":    BenchmarkParallelPut,
		"ParallelDelete": BenchmarkParallelDelete,
	}
	// BenchmarkSizes to use with each benchmark
	BenchmarkSizes = [][]int{
		// count, keySize, valueSize
		{1024, 32, 32},
		{1024, 256, 256},
		{1024, 2 * units.KiB, 2 * units.KiB},
	}
)

// Writes size data into the db in order to setup reads in subsequent tests.
func SetupBenchmark(b testing.TB, count int, keySize, valueSize int) ([][]byte, [][]byte) {
	require := require.New(b)

	b.Helper()

	r := rand.New(rand.NewSource(0))

	keys := make([][]byte, count)
	values := make([][]byte, count)
	for i := 0; i < count; i++ {
		keyBytes := make([]byte, keySize)
		valueBytes := make([]byte, valueSize)
		_, err := r.Read(keyBytes) // #nosec G404
		require.NoError(err)
		_, err = r.Read(valueBytes) // #nosec G404
		require.NoError(err)
		keys[i], values[i] = keyBytes, valueBytes
	}
	return keys, values
}

// BenchmarkGet measures the time it takes to get an operation from a database.
func BenchmarkGet(b *testing.B, db database.Database, keys, values [][]byte) {
	require.NotEmpty(b, keys)
	count := len(keys)

	require := require.New(b)

	for i, key := range keys {
		value := values[i]
		require.NoError(db.Put(key, value))
	}

	b.ResetTimer()

	// Reads b.N values from the db
	for i := 0; i < b.N; i++ {
		_, err := db.Get(keys[i%count])
		require.NoError(err)
	}
}

// BenchmarkPut measures the time it takes to write an operation to a database.
func BenchmarkPut(b *testing.B, db database.Database, keys, values [][]byte) {
	require.NotEmpty(b, keys)
	count := len(keys)

	// Writes b.N values to the db
	for i := 0; i < b.N; i++ {
		require.NoError(b, db.Put(keys[i%count], values[i%count]))
	}
}

// BenchmarkDelete measures the time it takes to delete a (k, v) from a database.
func BenchmarkDelete(b *testing.B, db database.Database, keys, values [][]byte) {
	require.NotEmpty(b, keys)
	count := len(keys)

	require := require.New(b)

	// Writes random values of size _size_ to the database
	for i, key := range keys {
		value := values[i]
		require.NoError(db.Put(key, value))
	}

	b.ResetTimer()

	// Deletes b.N values from the db
	for i := 0; i < b.N; i++ {
		require.NoError(db.Delete(keys[i%count]))
	}
}

// BenchmarkBatchPut measures the time it takes to batch put.
func BenchmarkBatchPut(b *testing.B, db database.Database, keys, values [][]byte) {
	require.NotEmpty(b, keys)
	count := len(keys)

	batch := db.NewBatch()
	for i := 0; i < b.N; i++ {
		require.NoError(b, batch.Put(keys[i%count], values[i%count]))
	}
}

// BenchmarkBatchDelete measures the time it takes to batch delete.
func BenchmarkBatchDelete(b *testing.B, db database.Database, keys, _ [][]byte) {
	require.NotEmpty(b, keys)
	count := len(keys)

	batch := db.NewBatch()
	for i := 0; i < b.N; i++ {
		require.NoError(b, batch.Delete(keys[i%count]))
	}
}

// BenchmarkBatchWrite measures the time it takes to batch write.
func BenchmarkBatchWrite(b *testing.B, db database.Database, keys, values [][]byte) {
	require.NotEmpty(b, keys)

	require := require.New(b)

	batch := db.NewBatch()
	for i, key := range keys {
		value := values[i]
		require.NoError(batch.Put(key, value))
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		require.NoError(batch.Write())
	}
}

// BenchmarkParallelGet measures the time it takes to read in parallel.
func BenchmarkParallelGet(b *testing.B, db database.Database, keys, values [][]byte) {
	require.NotEmpty(b, keys)
	count := len(keys)

	require := require.New(b)

	for i, key := range keys {
		value := values[i]
		require.NoError(db.Put(key, value))
	}

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for i := 0; pb.Next(); i++ {
			_, err := db.Get(keys[i%count])
			require.NoError(err)
		}
	})
}

// BenchmarkParallelPut measures the time it takes to write to the db in parallel.
func BenchmarkParallelPut(b *testing.B, db database.Database, keys, values [][]byte) {
	require.NotEmpty(b, keys)
	count := len(keys)

	b.RunParallel(func(pb *testing.PB) {
		// Write N values to the db
		for i := 0; pb.Next(); i++ {
			require.NoError(b, db.Put(keys[i%count], values[i%count]))
		}
	})
}

// BenchmarkParallelDelete measures the time it takes to delete a (k, v) from the db.
func BenchmarkParallelDelete(b *testing.B, db database.Database, keys, values [][]byte) {
	require.NotEmpty(b, keys)
	count := len(keys)

	require := require.New(b)
	for i, key := range keys {
		value := values[i]
		require.NoError(db.Put(key, value))
	}
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		// Deletes b.N values from the db
		for i := 0; pb.Next(); i++ {
			require.NoError(db.Delete(keys[i%count]))
		}
	})
}
