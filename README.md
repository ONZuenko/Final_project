# Final_project
В качестве итогового аттестационного проекта был выбран проект 5. 
В нем из большого файла - порядка шести с половиной миллиона записей - нужно было сделать агрегированную выборку и проанализировать данные.
Выборка должна содержать информацию о процентаже поездок для разного количества пассажиров за каждый день,
а также о минимальной и максимальной стоимости поездок для каждой группы.
Кроме того, необходимо было сдалеть проанализировать зависимости суммы чаевых от количества пассажиров и дальности поездок.

В качестве технологического стека из предложенных вариантов была выбрана Scala, как наиболее подходящий для данной задачи инструмент.
Реализовывался проект в среде IntelliJ IDEA 2022.2.3.

В представленный проект входит Scala проект, содержащий программный модуль, входные данные в виде csv файла, выходные данные в форматах parquet и csv.
Также приложен анализ данных, выполненный средствами Excel, и презентация проекта.

Алгоритм
На первом шаге выполняется импорт необходимых библиотек и настройка spark.
Н втором шаге создается схема, в которую будуд загружены данные.
На третьем шаге выполняется непосредственно загрузка данных из файла.
На четвертом шаге создается рабочий датафрейм, в который входят только нужные столбцы, и удаляются пустый строки.
На пятом шаге преобразуется формат даты.
На шестом шаге удаляются данные до 2020 годя, так как их очень мало для информативной выборки, скорее всего, они попали туда случкйно, и из можно считать шумом.
На седьмом шаге выполняется основное задание проекта, а именно, подсчет процентов поездок по количеству человек в машине,
а также нахождение самых дорогих и самых дешевых поездок для каждой группы.
На заключительном шаге результат выгружается в формат parquet.
А также создаются и выгружаются вспомогательные выборки в формате csv для построения графиков и анализа их в Excel.

PS. Исходные данные и jar файл не удалось загрузить из-за ограничения по размеру.
