// Copyright (C) 2019-2024, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package dbtest

import (
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ava-labs/avalanchego/database"
	// "github.com/ava-labs/avalanchego/utils/units"
)

/*
_MerkleDB_DBInterface/merkledb_2_1024_pairs_32_keys_32_values_Get-10                           694.5n ± ∞ ¹   693.4n ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_2_1024_pairs_32_keys_32_values_Put-10                           71.19µ ± ∞ ¹   44.31µ ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_2_1024_pairs_32_keys_32_values_Delete-10                        2.772µ ± ∞ ¹   3.079µ ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_2_1024_pairs_32_keys_32_values_BatchPut-10                      306.0n ± ∞ ¹   308.7n ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_2_1024_pairs_32_keys_32_values_BatchWrite-10                   10.093m ± ∞ ¹   5.986m ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_2_1024_pairs_32_keys_32_values_ParallelPut-10                   71.45µ ± ∞ ¹   49.25µ ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_2_1024_pairs_32_keys_32_values_BatchDelete-10                   257.7n ± ∞ ¹   295.6n ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_2_1024_pairs_32_keys_32_values_ParallelGet-10                   1.259µ ± ∞ ¹   1.254µ ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_2_1024_pairs_32_keys_32_values_ParallelDelete-10                3.111µ ± ∞ ¹   3.751µ ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_4_1024_pairs_32_keys_32_values_ParallelPut-10                   51.03µ ± ∞ ¹   33.30µ ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_4_1024_pairs_32_keys_32_values_Get-10                           691.2n ± ∞ ¹   695.7n ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_4_1024_pairs_32_keys_32_values_Put-10                           51.67µ ± ∞ ¹   33.19µ ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_4_1024_pairs_32_keys_32_values_Delete-10                        2.730µ ± ∞ ¹   3.191µ ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_4_1024_pairs_32_keys_32_values_BatchPut-10                      310.2n ± ∞ ¹   308.8n ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_4_1024_pairs_32_keys_32_values_BatchWrite-10                    7.286m ± ∞ ¹   3.977m ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_4_1024_pairs_32_keys_32_values_BatchDelete-10                   256.9n ± ∞ ¹   296.0n ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_4_1024_pairs_32_keys_32_values_ParallelGet-10                   1.263µ ± ∞ ¹   1.219µ ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_4_1024_pairs_32_keys_32_values_ParallelDelete-10                3.003µ ± ∞ ¹   3.770µ ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_16_1024_pairs_32_keys_32_values_BatchDelete-10                  249.7n ± ∞ ¹   268.6n ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_16_1024_pairs_32_keys_32_values_ParallelGet-10                  1.247µ ± ∞ ¹   1.219µ ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_16_1024_pairs_32_keys_32_values_ParallelDelete-10               3.106µ ± ∞ ¹   3.694µ ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_16_1024_pairs_32_keys_32_values_Get-10                          691.3n ± ∞ ¹   693.3n ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_16_1024_pairs_32_keys_32_values_Put-10                          58.72µ ± ∞ ¹   33.43µ ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_16_1024_pairs_32_keys_32_values_Delete-10                       2.685µ ± ∞ ¹   2.988µ ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_16_1024_pairs_32_keys_32_values_BatchPut-10                     284.2n ± ∞ ¹   340.2n ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_16_1024_pairs_32_keys_32_values_BatchWrite-10                   5.458m ± ∞ ¹   2.899m ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_16_1024_pairs_32_keys_32_values_ParallelPut-10                  63.40µ ± ∞ ¹   34.14µ ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_256_1024_pairs_32_keys_32_values_ParallelGet-10                 1.238µ ± ∞ ¹   1.189µ ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_256_1024_pairs_32_keys_32_values_ParallelDelete-10              5.233µ ± ∞ ¹   5.306µ ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_256_1024_pairs_32_keys_32_values_BatchDelete-10                 248.0n ± ∞ ¹   273.7n ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_256_1024_pairs_32_keys_32_values_Put-10                         234.3µ ± ∞ ¹   129.1µ ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_256_1024_pairs_32_keys_32_values_Delete-10                      3.216µ ± ∞ ¹   2.899µ ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_256_1024_pairs_32_keys_32_values_BatchPut-10                    248.5n ± ∞ ¹   321.0n ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_256_1024_pairs_32_keys_32_values_BatchWrite-10                  4.721m ± ∞ ¹   2.365m ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_256_1024_pairs_32_keys_32_values_ParallelPut-10                 210.9µ ± ∞ ¹   113.0µ ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_256_1024_pairs_32_keys_32_values_Get-10                         719.3n ± ∞ ¹   644.2n ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_2_1024_pairs_256_keys_256_values_Get-10                         762.6n ± ∞ ¹   824.3n ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_2_1024_pairs_256_keys_256_values_Put-10                         96.66µ ± ∞ ¹   57.87µ ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_2_1024_pairs_256_keys_256_values_Delete-10                      3.115µ ± ∞ ¹   3.088µ ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_2_1024_pairs_256_keys_256_values_BatchPut-10                    347.6n ± ∞ ¹   436.7n ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_2_1024_pairs_256_keys_256_values_BatchWrite-10                  27.00m ± ∞ ¹   18.07m ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_2_1024_pairs_256_keys_256_values_ParallelPut-10                 99.01µ ± ∞ ¹   54.07µ ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_2_1024_pairs_256_keys_256_values_BatchDelete-10                 299.1n ± ∞ ¹   285.5n ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_2_1024_pairs_256_keys_256_values_ParallelGet-10                 1.318µ ± ∞ ¹   1.314µ ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_2_1024_pairs_256_keys_256_values_ParallelDelete-10              3.903µ ± ∞ ¹   3.621µ ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_4_1024_pairs_256_keys_256_values_ParallelGet-10                 1.346µ ± ∞ ¹   1.320µ ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_4_1024_pairs_256_keys_256_values_ParallelDelete-10              3.540µ ± ∞ ¹   3.607µ ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_4_1024_pairs_256_keys_256_values_BatchDelete-10                 283.9n ± ∞ ¹   295.0n ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_4_1024_pairs_256_keys_256_values_Put-10                         73.58µ ± ∞ ¹   35.78µ ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_4_1024_pairs_256_keys_256_values_Delete-10                      2.993µ ± ∞ ¹   2.949µ ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_4_1024_pairs_256_keys_256_values_BatchPut-10                    315.5n ± ∞ ¹   345.3n ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_4_1024_pairs_256_keys_256_values_BatchWrite-10                  16.89m ± ∞ ¹   10.08m ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_4_1024_pairs_256_keys_256_values_ParallelPut-10                 74.75µ ± ∞ ¹   38.39µ ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_4_1024_pairs_256_keys_256_values_Get-10                         756.7n ± ∞ ¹   831.3n ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_16_1024_pairs_256_keys_256_values_Get-10                        760.3n ± ∞ ¹   802.0n ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_16_1024_pairs_256_keys_256_values_Put-10                        83.15µ ± ∞ ¹   35.96µ ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_16_1024_pairs_256_keys_256_values_Delete-10                     2.978µ ± ∞ ¹   2.990µ ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_16_1024_pairs_256_keys_256_values_BatchPut-10                   349.3n ± ∞ ¹   398.9n ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_16_1024_pairs_256_keys_256_values_BatchWrite-10                11.134m ± ∞ ¹   5.869m ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_16_1024_pairs_256_keys_256_values_ParallelPut-10                74.82µ ± ∞ ¹   38.36µ ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_16_1024_pairs_256_keys_256_values_BatchDelete-10                283.0n ± ∞ ¹   293.5n ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_16_1024_pairs_256_keys_256_values_ParallelGet-10                1.313µ ± ∞ ¹   1.314µ ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_16_1024_pairs_256_keys_256_values_ParallelDelete-10             3.442µ ± ∞ ¹   3.586µ ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_256_1024_pairs_256_keys_256_values_ParallelDelete-10            6.144µ ± ∞ ¹   5.661µ ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_256_1024_pairs_256_keys_256_values_BatchDelete-10               280.1n ± ∞ ¹   296.7n ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_256_1024_pairs_256_keys_256_values_ParallelGet-10               1.330µ ± ∞ ¹   1.263µ ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_256_1024_pairs_256_keys_256_values_Delete-10                    3.563µ ± ∞ ¹   2.993µ ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_256_1024_pairs_256_keys_256_values_BatchPut-10                  418.6n ± ∞ ¹   451.0n ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_256_1024_pairs_256_keys_256_values_BatchWrite-10                9.616m ± ∞ ¹   4.044m ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_256_1024_pairs_256_keys_256_values_ParallelPut-10               255.1µ ± ∞ ¹   114.3µ ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_256_1024_pairs_256_keys_256_values_Get-10                       767.1n ± ∞ ¹   741.8n ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_256_1024_pairs_256_keys_256_values_Put-10                       270.2µ ± ∞ ¹   132.6µ ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_2_1024_pairs_2048_keys_2048_values_Get-10                       8.040µ ± ∞ ¹   1.854µ ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_2_1024_pairs_2048_keys_2048_values_Put-10                       272.9µ ± ∞ ¹   136.9µ ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_2_1024_pairs_2048_keys_2048_values_Delete-10                   13.974µ ± ∞ ¹   3.464µ ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_2_1024_pairs_2048_keys_2048_values_BatchPut-10                  1.812µ ± ∞ ¹   1.792µ ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_2_1024_pairs_2048_keys_2048_values_BatchWrite-10                150.3m ± ∞ ¹   110.0m ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_2_1024_pairs_2048_keys_2048_values_ParallelPut-10               215.2µ ± ∞ ¹   144.8µ ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_2_1024_pairs_2048_keys_2048_values_BatchDelete-10               618.8n ± ∞ ¹   842.2n ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_2_1024_pairs_2048_keys_2048_values_ParallelGet-10               2.488µ ± ∞ ¹   1.815µ ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_2_1024_pairs_2048_keys_2048_values_ParallelDelete-10           14.778µ ± ∞ ¹   4.282µ ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_4_1024_pairs_2048_keys_2048_values_BatchDelete-10               597.8n ± ∞ ¹   815.1n ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_4_1024_pairs_2048_keys_2048_values_ParallelGet-10               2.486µ ± ∞ ¹   1.880µ ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_4_1024_pairs_2048_keys_2048_values_ParallelDelete-10            9.793µ ± ∞ ¹   3.649µ ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_4_1024_pairs_2048_keys_2048_values_ParallelPut-10              176.86µ ± ∞ ¹   83.78µ ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_4_1024_pairs_2048_keys_2048_values_Get-10                       6.685µ ± ∞ ¹   1.911µ ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_4_1024_pairs_2048_keys_2048_values_Put-10                      227.86µ ± ∞ ¹   89.69µ ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_4_1024_pairs_2048_keys_2048_values_Delete-10                    7.563µ ± ∞ ¹   3.246µ ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_4_1024_pairs_2048_keys_2048_values_BatchPut-10                  1.615µ ± ∞ ¹   1.863µ ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_4_1024_pairs_2048_keys_2048_values_BatchWrite-10                95.85m ± ∞ ¹   58.44m ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_16_1024_pairs_2048_keys_2048_values_BatchDelete-10              471.2n ± ∞ ¹   612.1n ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_16_1024_pairs_2048_keys_2048_values_ParallelGet-10              3.265µ ± ∞ ¹   1.822µ ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_16_1024_pairs_2048_keys_2048_values_ParallelDelete-10          13.764µ ± ∞ ¹   4.175µ ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_16_1024_pairs_2048_keys_2048_values_BatchWrite-10               57.39m ± ∞ ¹   31.69m ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_16_1024_pairs_2048_keys_2048_values_ParallelPut-10             186.25µ ± ∞ ¹   66.70µ ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_16_1024_pairs_2048_keys_2048_values_Get-10                      8.926µ ± ∞ ¹   1.642µ ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_16_1024_pairs_2048_keys_2048_values_Put-10                     227.60µ ± ∞ ¹   73.60µ ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_16_1024_pairs_2048_keys_2048_values_Delete-10                  10.532µ ± ∞ ¹   3.167µ ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_16_1024_pairs_2048_keys_2048_values_BatchPut-10                 1.422µ ± ∞ ¹   1.800µ ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_256_1024_pairs_2048_keys_2048_values_Put-10                    1066.7µ ± ∞ ¹   149.2µ ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_256_1024_pairs_2048_keys_2048_values_Delete-10                 53.025µ ± ∞ ¹   3.350µ ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_256_1024_pairs_2048_keys_2048_values_BatchPut-10                1.694µ ± ∞ ¹   1.398µ ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_256_1024_pairs_2048_keys_2048_values_BatchWrite-10              57.38m ± ∞ ¹   17.64m ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_256_1024_pairs_2048_keys_2048_values_ParallelPut-10             708.5µ ± ∞ ¹   131.3µ ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_256_1024_pairs_2048_keys_2048_values_Get-10                     7.741µ ± ∞ ¹   1.515µ ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_256_1024_pairs_2048_keys_2048_values_ParallelGet-10             3.131µ ± ∞ ¹   1.707µ ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_256_1024_pairs_2048_keys_2048_values_ParallelDelete-10        140.642µ ± ∞ ¹   6.125µ ± ∞ ¹        ~ (p=1.000 n=1) ²
_MerkleDB_DBInterface/merkledb_256_1024_pairs_2048_keys_2048_values_BatchDelete-10             672.0n ± ∞ ¹   896.2n ± ∞ ¹        ~ (p=1.000 n=1) ²
geomean                                                                                        12.51µ         8.404µ        -32.80%
*/

var (
	// Benchmarks is a list of all database benchmarks
	Benchmarks = map[string]func(b *testing.B, db database.Database, keys, values [][]byte){
		"Get":            BenchmarkGet,
		"Put":            BenchmarkPut,
		"Delete":         BenchmarkDelete,
		// "BatchPut":       BenchmarkBatchPut,
		// "BatchDelete":    BenchmarkBatchDelete,
		"BatchWrite":     BenchmarkBatchWrite,
		// "ParallelGet":    BenchmarkParallelGet,
		// "ParallelPut":    BenchmarkParallelPut,
		// "ParallelDelete": BenchmarkParallelDelete,
		// "Realistic":      BenchmarkRealisticWorkload,
	}
	// BenchmarkSizes to use with each benchmark
	BenchmarkSizes = [][]int{
		// count, keySize, valueSize
		{1024, 32, 32},
		// {1024, 256, 256},
		// {1024, 2 * units.KiB, 2 * units.KiB},
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


func BenchmarkRealisticWorkload(b *testing.B, db database.Database, keys, values [][]byte) {
	require := require.New(b)
	require.NotEmpty(keys)

	count := len(keys)
	existingEntries := int(float64(count) * 0.8) // Declare existingEntries explicitly

	// Pre-fill database with 80% of keys to simulate existing data
	for i := 0; i < existingEntries; i++ {
		require.NoError(db.Put(keys[i], values[i]))
	}

	b.ResetTimer()

	const opsPerBatch = 1000      // Number of operations per batch (K)
	const writeRatio = 0.2        // 20% writes
	const existingKeyRatio = 0.8  // 80% existing keys

	rnd := rand.New(rand.NewSource(42)) // Use rnd consistently

	for batchNum := 0; batchNum < b.N; batchNum++ {
		batch := db.NewBatch()
		for op := 0; op < opsPerBatch; op++ {
			if rnd.Float64() < writeRatio {
				// Write operation
				var key []byte
				if rnd.Float64() < existingKeyRatio {
					// Existing key (80%)
					key = keys[rnd.Intn(existingEntries)]
				} else {
					// New key (20%)
					key = keys[existingEntries+rnd.Intn(count-existingEntries)]
				}
				value := values[rnd.Intn(len(values))]
				require.NoError(batch.Put(key, value))
			} else {
				// Read operation
				var key []byte
				if rnd.Float64() < existingKeyRatio {
					// Existing key (80%)
					key = keys[rnd.Intn(existingEntries)]
				} else {
					// Non-existing key (20%)
					key = keys[existingEntries+rnd.Intn(count-existingEntries)]
				}
				_, _ = db.Get(key) // ignore errors for non-existing keys
			}
		}
		require.NoError(batch.Write())
	}
}
