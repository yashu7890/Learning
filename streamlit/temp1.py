


# you can write to stdout for debugging purposes, e.g.
# print("this is a debug message")

def solution(T):
    # write your code in Python 3.6
    num_days= int(len(T)/4)
    num_days= int(len(T)/4)
    print(num_days)
    for i in range(0,len(T),num_days):
        print(T[i:i+num_days])
    return("pass") 

print(solution([-3,-14,1,2,3,4,5,6]))