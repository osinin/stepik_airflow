from ast import literal_eval

a = 202311
print(type(a))
if type(a) is not list:
    a = [str(a)]
    print(a, type(a))


number = literal_eval("42")
print(number, type(number))
string = literal_eval("'Hello, World!'")
print(string, type(string))
list_obj = literal_eval("[1, 2, 3]")
print(list_obj, type(list_obj))