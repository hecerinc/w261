from functools import reduce

def combineCtrs(c1, c2):
	return (c1[0] + c2[0], c1[1] + c2[1])

def basicAvg(nums):
	b = reduce(combineCtrs, map(lambda x: (x, 1), nums))
	return b[0]/float(b[1])


print(basicAvg(list(range(1, 11))))


def partitionCtr(nums):
	# Compute sumCounter for partition
	sumCount = [0, 0]
	for num in nums:
		sumCount[0] += num
		sumCount[1] += 1
	return [sumCount]


def fastAvg(nums):
	# Compute the average
	sumCount = nums.mapPartitions(partitionCtr).reduce(combineCtrs)
	return sumCount[0] / float(sumCount[1])

print(fastAvg(list(range(1, 11))))
