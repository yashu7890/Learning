
def prod_of_ele(arr):
    prod=1
    zero_count=0
    for i in arr:
        if i!=0:
            prod=prod*i
        else:
            zero_count+=1
            
    final=[]
    for i in arr:
        if i!=0:
            if zero_count==0:
                final.append(prod/i)
            else:
                final.append(0)
        else:
            if zero_count-1>0:
                final.append(0)
            else:
                final.append(prod)

    return final 

#better solution
# timecomplexity is o(n)
# space complexity is o(1)
def prod_of_ele(arr):
    prod_of_all_numbers=1
    final=[]
    zero_count=0
    for i in arr:
        if i!=0:
            prod_of_all_numbers=prod_of_all_numbers*i
        else:
            zero_count+=1
            
    if zero_count>1:
        return([0 for i in arr])
    else:
        for i in arr:
            if zero_count==1:
                if i==0:
                    final.append(prod_of_all_numbers)
                else:
                    final.append(prod_of_all_numbers/i)
            else:
                final.append(prod_of_all_numbers/i)

    return final 

print(prod_of_ele([1,2,3,4]))
print(prod_of_ele([1,2,3,-4]))
print(prod_of_ele([1,-2,3,-4]))
print(prod_of_ele([1,2,0,4]))
print(prod_of_ele([1,2,0,4,0,5]))