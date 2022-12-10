#using dynamic programing
def max_loot(houses,n):
    if n==0:
        return 0
    if n==1:
        return houses[0]
    if n==2:
        return max(houses[0],houses[1])
    
    dp=[0]*n
    
    dp[0]=houses[0]
    dp[1]=max(houses[0],houses[1])
    print(houses)
    print(dp)
    for i in range(2,n):
        dp[i]=max(houses[i]+dp[i-2],dp[i-1])
        print(i,"-------",houses[i]+dp[i-2],dp[i-1],dp)
    return dp[-1]

#using recursion:

def max_loot(houses,n):
    pass
print(max_loot([6,7,1,3,8,2,4],7))
print(max_loot([1,2,8,3,8,2,4],7))
print(max_loot([2,4,1,3,8,2,4],7))