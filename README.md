# hadoop-lab1

🚀 М21-502 Рыбкин Павел - Developer

====================================================

1. Разработка велась с использованием Hadoop
  ![image](https://user-images.githubusercontent.com/72603507/161383343-2b571588-ea02-486d-b17c-e6615589e605.png)
2. OS Centos
![image](https://user-images.githubusercontent.com/72603507/161383397-7842ac80-c147-401f-bdce-dc513d9713af.png)
3. Для сборки используется maven плагин для Intellij IDEA
✔️ 

====================================================

2. Базовое решение представляет следующие возможности:

 🍕 Генерация исходных данных скрипт-файл genMyDAta.sh; ✔️

 🍔 Mapper - HW1Mapper;✔️
 
 🍈 Reducer - HW1Reducer;✔️
 
 🆎 Partitioner - HW1Partitioner;✔️

 📄 Тестовые файлы для тестирования Mapper и Reducer. ✔️

====================================================

3. Результаты тестирования представлены на скришотах:


![image_2022-04-02_14-38-51](https://user-images.githubusercontent.com/72603507/161383112-67129e5e-b79c-468e-8ca8-e22b3418253b.png)
![image_2022-04-02_14-36-32](https://user-images.githubusercontent.com/72603507/161383105-e5e46374-dd42-4b24-b4ce-2e188ff71c91.png)


====================================================

4. Базовое решение представляет следующие возможности:

 1) Генерация данных с помощью скрипта ./genMyDAta.sh
 2) Build программы
 3) Проверить dfs на наличие output и input директорий обнулить при необходимости
 
 ```Shell
 YOUT_HADOOP_PATH/bin/hdfs dfs -ls
 ```

Found 2 items

drwxr-xr-x   - root supergroup          0 2022-03-30 12:25 input

drwxr-xr-x   - root supergroup          0 2022-03-30 12:25 output

 ```Shell
 YOUT_HADOOP_PATH/bin/hdfs dfs -rm -r input

 YOUT_HADOOP_PATH/bin/hdfs dfs -rm -r output

 YOUT_HADOOP_PATH/bin/hdfs dfs -put input
 ```


 4) Запуск обработки

 ```Shell
 YOUT_HADOOP_PATH/bin/yarn jar ./target/lab1-1.0-SNAPSHOT-jar-with-dependencies.jar input output
 ```

 5) Просмотр результата localhost:50070/explorer.html#/user/root/

====================================================

5. Процесс выполнения программы представлен на скришлотах:

![image_2022-04-02_14-48-48](https://user-images.githubusercontent.com/72603507/161383111-09ad5173-4f86-4a99-b0cd-1d6a6456389b.png)
![image_2022-04-02_14-49-26](https://user-images.githubusercontent.com/72603507/161383107-083ab9f5-a3d1-4395-a402-4877460ea77c.png)

====================================================

6. Результат выполнения представлен на скришотах


![image_2022-04-02_14-54-26](https://user-images.githubusercontent.com/72603507/161383110-70d2411a-11e4-4a4e-9027-39b990cdd79f.png)
![image_2022-04-02_14-55-01](https://user-images.githubusercontent.com/72603507/161383103-65ef7eef-f1ae-4afc-b806-c1d27051ba71.png)
