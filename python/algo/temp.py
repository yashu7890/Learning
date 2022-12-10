# #using dynamic programing
def minJumps(arr, n):
  # The number of jumps needed to reach the starting index is 0
  if (n < 1):
    return 0
  
  # Return -1 if not possible to jump
  if (arr[0] == 0):
    return -1
  
  # initialization
  # stores all time the maximal reachable index in the array
  maxReach = arr[0] 
  # stores the amount of steps we can still take
  step = arr[0]
  # stores the amount of jumps necessary to reach that maximal reachable position
  jump = 1
  
  # Start traversing array
  print(arr)
  for i in range(1, n):
    # Check if we have reached the end of the array
    if (i == n-1):
      return jump
    
    # updating maxReach
    maxReach = max(maxReach, i + arr[i])
    
    
    # we use a step to get to the current index
    step -= 1;
    print("index",i,"value",arr[i],"max reach",maxReach,"step",step)
    # If no further steps left
    if (step == 0):
    #   print("if",arr[i])
      print(step,maxReach,jump)
      # we must have used a jump
      jump += 1
       
      # Check if the current index or lesser index is the maximum reach point from the previous indexes
      if(i >= maxReach):
        return -1
  
      # re-initialize the steps to the amount
      # of steps to reach maxReach from position i.
      step = maxReach - i
      print(step,maxReach,jump)

  return -1
  
    
print(minJumps([6,7,1,3,8,2,4],7))
print(minJumps([1,2,8,3,8,2,4],7))
print(minJumps([2,4,1,3,8,2,4],7))
print(minJumps([1, 3, 5, 2, 3, 2, 6, 7, 6, 8, 9],11))
