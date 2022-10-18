# defining the libraries
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
# %matplotlib inline
  
# No of Data points
N = 500
  
# initializing random values
data = [3.5,3.5,24.5,35,245,346.5,546,693,724.5,934.5,987,1295,1319.5,1407,1442,1571.5,1830.5,2348.5,2453.5,2541,2660,2730,2737,3664.5,3787,3801,3881.5,3983,4427.5,4438,5407.5,5579,6426,6548.5,7574,7731.5,7955.5,8529.5,9625,10426.5,11693.5,12001.5,12404,13114.5,13776,14868,15032.5,16572.5,17146.5,17699.5,17780,18567.5,19239.5,20436.5,20555.5,20601,20779.5,21623,23128,23142,23268,23947,25095,27132,27678,29463,29757,29886.5,30005.5,30656.5,31864,32266.5,32938.5,35357,36666,39154.5,41303.5,42042,42297.5,42490,43291.5,43610,44114,45304,45419.5,45766,46753,51009,66104.5,79901.5]
  
# getting data of the histogram
count, bins_count = np.histogram(data, bins=90)
  
# finding the PDF of the histogram using count values
pdf = count / sum(count)
  
# using numpy np.cumsum to calculate the CDF
# We can also find using the PDF values by looping and adding
cdf = np.cumsum(pdf)
  
# plotting PDF and CDF
print(cdf)
plt.plot(bins_count[1:], cdf, color="red",label="CDF")
plt.legend()
plt.ylim(0,1)
plt.show()