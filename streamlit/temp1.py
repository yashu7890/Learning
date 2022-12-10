


# you can write to stdout for debugging purposes, e.g.
# print("this is a debug message")

# def solution(T):
#     # write your code in Python 3.6
#     num_days= int(len(T)/4)
#     num_days= int(len(T)/4)
#     print(num_days)
#     for i in range(0,len(T),num_days):
#         print(T[i:i+num_days])
#     return("pass") 

# print(solution([-3,-14,1,2,3,4,5,6]))


t= [1, 3, 5, 8, 1, 2, 6, 7, 6, 8, 9]
# t=[1,1,1,1,1,1,1,1,1,1,1,1]
t= [1, 3, 9, 8, 5, 2, 6, 7, 6, 8, 9]
t=[2, 6, 3, 7, 9, 2, 6, 7]
# 1,3,1,2,7,
tempLen=len(t)
s=0
jumps=0
while (s<len(t) and tempLen>0):
    temp=t[s]

    s+=temp
    jumps+=1
    tempLen-s
print(jumps)

