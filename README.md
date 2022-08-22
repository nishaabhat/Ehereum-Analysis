# Ehereum-Analysis - Big Data Processing
## Time Analysis ##
#### Context ####
From the Ethereum transactions dataset, count of transactions with (month, year) as key is calculated and a bar plot of count vs (month, year) is plotted on excel. Also, a monthly average value is calculated for all the data available in the data set and a bar plot of average vs (month, year) is plotted on excel.
#### Code ####
Separate programs are created for count of transactions and average of transactions. MRJob is imported for the Map-Reduce programs.
For count of transactions, the mapper is designed to return the combination of month, year as the key along with the count as the value. The combiner accepts them as word (key) and values (count). It performs addition operation on the count returning the sum of the counts for that key. Reducer performs exactly same operation as the combiner making the operation run faster than it would without the combiner.
Similar structure is designed to calculate the average of transactions. The mapper returns the combination of month, year as the key and value of transaction as the value. The combiner calculates the total of count of transaction and the total of value of transaction at every iteration. The reducer accepts month, year as key and count, total as value. It then calculates the average.
#### Conclusion ####
Count of transaction starts to gain good traction post May 2017, reaching its peak in the month of Jan 2018.
Average of transaction shows a downtrend through the years, mostly due to smaller values being transacted in recent years.

## Top ten most popular services ##
#### Context ####
Top smart contacts are determined by the total Ether received for the that contract. Total Ether is given by the transaction value from transactions dataset.
#### Code ####
Two methods are defined to fetch clean datasets. A transactions dataset must have 7 fields while a contracts dataset must have 5 fields. Failing to meet this criteria will render the data useless. Upon fetching the clean datasets, transactions data is mapped to get fields - to_address and value of transaction; contracts data is mapped to get field – address. Next, an aggregation is performed on transactions using to_address as the key. Then, join operation is performed to match the addresses in both the tables, thereby filtering out addresses not available within contracts. Finally, the list is sorted in the descending order of the transaction value to fetch the 10 most popular contracts.

## Top ten most active miners ##
#### Context ####
The blocks dataset contains the column miners and size of the block mined that are used to identify top ten active miners. The assumption is that larger the size of the block mined, more active the miner is.
#### Code ####
Blocks dataset is filtered and only data with all 9 fields and a non-zero block size is fetched. It is then aggregated on the key – field miner to get total size of block mined by that miner. The data is then sorted in descending order and the top ten miners are printed on console.

## Wash Trading ##
#### Context ####
Wash trading, also referred to as round trip trading is fraudulent transactions on Ethereum that are performed to avoid market risks and retain an address’s market position. Two main criteria of wash traders are,
* Trading among themselves : Having same sender and receiver or performing cyclic transactions among a group of people.
* When cyclic transactions are performed they are of similar values so the wash traders can retain their market position.
Upon examining the transactions dataset, attempting to search for rows with same to_address and from_address, provided inconclusive results. Many addresses have performed at least one transaction from and to themselves. Asserting the correctness of this result proved to be close to impossible with the given dataset.
Also identifying a cyclic transaction seems to be a complicated process – a network of from_address and to_address must be constructed with directional edges. Each edge needs to be given a weight that is equivalent to the transaction value. A cyclic transaction is possibly
fraudulent if a total value transacted remains intact throughout the cycle – this could further involve multiple cycles.
The code attached with this report attempts to identify how many times an address has sent and received Ether of similar values.
#### Code ####
Assumption made for this program is that if an address has sent and received similar values of transaction, it is possible that they are fraudulent. For example, if address A1 has sent Ether of value 100 and received Ether of value 102, it is possible that a cyclic transaction was established, deeming them wash traders.
In the program, a window of +/-5% is considered – if the ether received is within the range of Ether sent +/- 5, then it is considered as fraudulent.
The mapper in this program returns to_address as key and value of transaction as value. It also returns from_address as key and value of transaction as value. To differentiate between the two, keywords ‘TO’ and ‘FROM’ are used.
The reducer first aggregates all the ‘to’ and ‘from’ transaction values for a given address. For every address, a total value of transaction sent, and a total value of transaction received is calculated.
E.g.: (0x3f5ce5fbfe3e9af3971dd833d26ba9b5c936f0be, 39644316438351200000, 39644316488351200099)
Then, the two totals are compared to check if the meet the assumption we made in the beginning. The number of times an address has similar ‘to ‘and ‘from’ transaction values is noted. First reducer returns the address and the count.
e.g. (0x3f5ce5fbfe3e9af3971dd833d26ba9b5c936f0be, 852230)
In the above example 0x3f5ce5fbfe3e9af3971dd833d26ba9b5c936f0be had 852230 transactions that had similar ‘to’ and ‘from’ transaction values.
The second reducer only sorts the addresses in the descending order of the count and returns a string of address and count.
It is highly possible that an address is transacting similar values legally. The given dataset does not provide enough information to authenticate a transaction. Also, the above program does not take date of transaction into consideration. It is possible that an address who experience loss in year 1, made some smart investments in year 2 to recover the loss. Hence, it cannot be concluded that the address obtained (above) are wash traders.

## Fork Analysis ##
Who profited from fork: Fork_Profit.py
#### Context ####
Byzantium fork was created on 16th October 2017. This particular fork is chosen as it seems to have happened at almost the mid of the timeline available in the dataset.
The variation in average gas price is examined during the time of fork. Two days before and after the fork are examined to identify addresses that may have profited from the fork.
#### Code ####
Only those data belonging to the month of October 2017 are taken for the analysis. For each day, the gas price is aggregated and averaged. A line graph is then plotted on excel to analyse the price variations.
Output and conclusion
Figure viii: Average gas price variations
The gas price seems to have plummeted after fork (16th Oct).
In order to profit from the fork, an address must sell Ether before the fork. Also, if an address buys Ether after the fork, there are chances of profit.
#### Code - Fork_Profit.py ####
Two methods are defined to filter the dataset appropriately. To identify who may have sold ether before fork, the first method fetches from_address that transacted on 14th and 15th October. To identify who may have bought ether after fork, the second method fetches to_address that transacted on 16th and 17th October.
The transaction values are then aggregated using the address as key. The program will generate two lists – one with top 10 address who transacted highest values on 14th and 15th October, and the other with top 10 address who transacted highest values on 16th and 17th October.
These address have a good chance of profiting from the fork. However, further analysis on at what price the addresses in the ‘before’ list bought ether is required to be determine if they were profited. Similarly, price at which the address in the ‘after’ list sold the ether is important to determine if they were profited.

## Gas Guzzlers ##
#### Context ####
In order to examine the changes in the gas price, transactions dataset is analysed. To analyse how complexity evolves with the gas limit, blocks dataset id analysed.
#### Code ####
Three methods are defined to filter for clean data containing appropriate number of fields. First, using transaction dataset, an average gas price over time is calculated by aggregating gas price using month, year as key.
Next, mapper on blocks creates a dataset with block number, difficulty, gas used and time. To get all the valid blocks, the contracts dataset and the blocks dataset are joined on block number. The gas used and difficulty are aggregated using month, year as key.
#### Conclusion ####
The average gas price follows an overall down trend with time. However, there are some spikes every year – could be a seasonality – either Christmas or new years’.
The complexity and gas limit follow similar trend. Both start to rise since April 2017. However, since 2018, even though gas limit stays steady, complexity seems to fluctuate. The last few months in 2019 see a downtrend in complexity, while gas limit is steady. It could mean that in the early years of Ethereum, as gas limit increased, the complexity also increased. However, in recent years, there must be another factor that has enabled reduction in complexity as using same amount of gas seems less complex.

## Reference ##
* ethereum.org. 2022. History and Forks of Ethereum | ethereum.org. [online] Available at: <https://ethereum.org/en/history/#byzantium> [Accessed 15 April 2022].
* Victor, F. and Weintraud, A., 2021. etecting and Quantifying Wash Trading on Decentralized Cryptocurrency Exchanges. [online] Available at: <https://dl.acm.org/doi/abs/10.1145/3442381.3449824> [Accessed 15 April 2022].
Other coding references from the internet
