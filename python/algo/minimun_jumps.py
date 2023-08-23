# #using dynamic programing
def min_jumps(l):
    length=len(l)
    op=[]
    jumps=1
    index=0
    if length==0:
        return 0
    elif length==1:
        return 1
    else:
        while index<length:
            if l[index]>=length-1:
                return 1
            else:
                op.append(l[index])         
                t=max(l[index],l[index]+index)
                if (t>=length):
                    break

            index+=1


    return op

    
print(min_jumps([1, 3, 5, 8, 9, 2, 6, 7, 6, 8, 9]))
print(min_jumps([1,2,8,3,8,2,4]))
print(min_jumps([2,4,1,3,8,2,4]))

