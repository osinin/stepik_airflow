from jinja2 import Template

t = Template("Hello {{ something }}!") # Создадим шаблон
res = t.render(something="World") # Отрендерим результат
print(res)

t = Template("My favorite numbers: {% for n in range(1, 10) %}{{n}} " "{% endfor %}")
print(t.render())

nums = [n for n in range(2, 10, 2)]
print(nums)

x = int(input())
t = Template("{% for n in range(2, x, 2) %}{{n}} " "{% endfor %}")
print(t.render(x=x))
