// Параллельное программирование.cpp : Этот файл содержит функцию "main". Здесь начинается и заканчивается выполнение программы.
//

#include <vector>
#include <iterator>
#include <thread>
#include <algorithm>
#include <iostream>
#include <numeric>
#include <functional>
#include <queue>
#include <condition_variable>
#include <time.h>
#include <stdexcept>
#include <string>
#include <fstream>
#include <cstdio>
#include <sstream>
#include <stdio.h>

using namespace std;

//SafeQueue is a queue designed to work with "safely" with threads.
//It is a template class so you can work with any type of data you want
template<class T>
class SafeQueue {
	queue<T> q;
	condition_variable c;
	mutable mutex m;

	mutable mutex vector_m;
	vector<bool> everybody_works;

public:
	SafeQueue(int thread_num)
	{
		everybody_works.assign(thread_num, false);
	}

	SafeQueue(SafeQueue<T> const& other)
	{
		lock_guard<mutex> g(other.m);
		lock_guard<mutex> gv(other.vector_m);
		q = other.q;
		everybody_works = other.everybody_works;
	}

	SafeQueue(queue<T> const& default_queue, int thread_num)
	{
		q = default_queue;
		everybody_works.assign(thread_num, false);
	}

	void push(T val)
	{
		lock_guard<mutex> g(m);
		q.push(val);
		c.notify_one();
	}

	int size()
	{
		lock_guard<mutex> g(m);
		return q.size();
	}

	void set_me_working(int th, bool val)
	{
		lock_guard<mutex> g(vector_m);
		everybody_works[th] = val;
		c.notify_one();
	}

	bool is_everybody_working()
	{
		lock_guard<mutex> g(vector_m);
		return accumulate(everybody_works.begin(), everybody_works.end(), false);
	}

	T just_pop()
	{
		lock_guard<mutex> lk(m);
		if (q.empty())
			throw "No elems";
		T a = q.front();
		q.pop();
		return a;
	}

	bool wait_pop(T& a, T& b)
	{
		unique_lock<mutex> lk(m);
		c.wait(lk, [this] { return q.size() > 1 || !is_everybody_working(); });
		if (q.empty())
		{
			throw "Ooops! Smth has gone wrong!";
		}
		if (q.size() == 1)
			return false;
		a = q.front();
		q.pop();
		b = q.front();
		q.pop();
		return true;
	}

};


//reading data one by one from two files and put the lesser one first
void thread_work(SafeQueue<string>& q, int my_num)
{
	while (true)
	{
		string a, b;
		bool go = q.wait_pop(a, b);
		if (!go) {
			break;
		}
		q.set_me_working(my_num, true);
		FILE* file1, * file2, * result;
		file1 = fopen(a.c_str(), "rb+");
		file2 = fopen(b.c_str(), "rb+");
		int buf1 = 0, buf2 = 0, c1, c2;
		string filename = to_string(std::hash<std::thread::id>{}(std::this_thread::get_id()));
		result = fopen(filename.c_str(), "wb+");
		while (true)
		{
			c1 = fread(&buf1, sizeof(int), 1, file1);
			c2 = fread(&buf2, sizeof(int), 1, file2);
			if (c1 * c2 > 0)
			{
				if (buf1 >= buf2)
				{
					fseek(file2, -int(sizeof(int)), SEEK_CUR);
					fwrite(&buf1, sizeof(int), 1, result);
				}
				else
				{
					fseek(file1, -int(sizeof(int)), SEEK_CUR);
					fwrite(&buf2, sizeof(int), 1, result);
				}
			}
			else
			{
				if (c1 == 0)
				{
					fwrite(&buf2, sizeof(int), 1, result);
					int abs = 1;
					int* buf_abs = new int[100000];
					while (abs != 0)
					{
						abs = fread(buf_abs, sizeof(int), 100000, file2);
						fwrite(buf_abs, sizeof(int), abs, result);
					}
					delete[] buf_abs;
				}
				else
				{
					fwrite(&buf1, sizeof(int), 1, result);
					int abs = 1;
					int* buf_abs = new int[10000];
					while (abs != 0)
					{
						abs = fread(buf_abs, sizeof(int), 10000, file1);
						fwrite(buf_abs, sizeof(int), abs, result);
					}
					delete[] buf_abs;
				}
				break;
			}
		}
		fclose(result);
		fclose(file1);
		fclose(file2);
		q.push(filename);
		remove(a.c_str());
		remove(b.c_str());
		q.set_me_working(my_num, false);
	}
	q.set_me_working(my_num, false);
}

// function for initialising work of merging all created files
void merged_sorted_file(SafeQueue<string>& q, int req_num_treads)
{
	cout << "beginning merging" << endl;
	queue<string> default_queue;
	int max_threads = thread::hardware_concurrency(); 
	int num_threads = min(max_threads, req_num_treads);
	vector<thread> threads(num_threads - 1);
	try
	{
		int tres, tstart, tend;
		auto start = std::chrono::steady_clock::now();

		for (int i = 0; i < num_threads - 1; i++)
		{
			threads[i] = thread(thread_work, ref(q),  i);;
		}
		thread_work(ref(q), num_threads - 1); 
		for_each(threads.begin(), threads.end(), mem_fn(&thread::join));
		auto end = std::chrono::steady_clock::now();
		auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
		cout << "Process 2 (merging)" << endl;
		cout << "Done by " << num_threads << " thread(s) in " << elapsed.count() << " millisecs." << endl;

	}

	catch (...)
	{
		cout << "that's a flop" << endl;
		for_each(threads.begin(), threads.end(), mem_fn(&thread::join));
		throw;
	}

}

//function to read given count of ints from file
int read_from(ifstream& in, int N, vector<int>& V, int pos, SafeQueue<string>& q) {
	int iter = 0;
	cout << "Thread " << this_thread::get_id() << endl;
	string filename = to_string(pos);
	ofstream out(filename, ios::binary);
	q.push(filename);
	while (in.peek() != EOF && iter < N) {
		int i;
		in.read((char*)&i, sizeof(int));
		iter++;
		V.push_back(i);
	}
	sort(V.begin(), V.end(), std::greater<int>());
	for (int i = 0; i < V.size(); i++) {
		out.write((char*)&V[i], sizeof(V[i]));
	}
	out.close();
	return iter;
}

// function to start reading process from file
void f(mutex& m, ifstream& in, int pos, int& count, SafeQueue<string>& q) {
	lock_guard<mutex> guard(m);
	vector<int> V;
	int cnt = read_from(in, count, V, pos, q);
	/*cout << "Количество прочитанных файлов " << cnt << endl;
	for (int i = 0; i < V.size(); i++) {
		cout << V[i] << " " << endl;
	}*/
}

// function to count all ints in file and give each threads required number of ints to read
long int count_of_int(ifstream& in) {
	in.seekg(0, ios::end);
	long int n = in.tellg();
	in.seekg(0);
	return n / sizeof(int);
}

//assign reading process
void process_1(ifstream& in, SafeQueue<string>& q) {
	mutex m;
	int max_threads = thread::hardware_concurrency();
	vector<thread> threads(max_threads - 1);
	int tres, tstart, tend;
	auto start = std::chrono::steady_clock::now();
	int full_cnt = (int)count_of_int(in);
	int cnt =  full_cnt / (max_threads - 1);
	for (int i = 0; i < max_threads - 2; i++)
	{
		threads[i] = thread(f,ref(m), ref(in), i * cnt, ref(cnt), ref(q));
	}
	int rest_cnt = full_cnt - cnt * (max_threads - 2);
	threads[max_threads - 2] = thread(f, ref(m), ref(in), (max_threads - 1) * cnt, ref(rest_cnt), ref(q));
	f(ref(m), ref(in), (max_threads - 1) * cnt, ref(rest_cnt), ref(q));
	for_each(threads.begin(), threads.end(), mem_fn(&thread::join));
	auto end = std::chrono::steady_clock::now();
	auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

	cout << "Process 1 (dividing)" << endl;
	cout << "Done by " << max_threads << " thread(s) in " << elapsed.count() << " millisecs." << endl;
}



int main()
{
	ofstream out("test1.dat", ios::binary);
	int N = 100;
	for (int i = 1; i < N + 1; i++) {
		out.write((char*)&i, sizeof(i));
	}
	out.close();
	ifstream in("test1.dat", ios::binary);
	queue<string> def_queue;
	int max_threads = thread::hardware_concurrency();
	SafeQueue<string> q(def_queue,max_threads);
	process_1(in, q);
	in.close();
	merged_sorted_file(q, 4);
	return 0;
}

// Запуск программы: CTRL+F5 или меню "Отладка" > "Запуск без отладки"
// Отладка программы: F5 или меню "Отладка" > "Запустить отладку"

// Советы по началу работы 
//   1. В окне обозревателя решений можно добавлять файлы и управлять ими.
//   2. В окне Team Explorer можно подключиться к системе управления версиями.
//   3. В окне "Выходные данные" можно просматривать выходные данные сборки и другие сообщения.
//   4. В окне "Список ошибок" можно просматривать ошибки.
//   5. Последовательно выберите пункты меню "Проект" > "Добавить новый элемент", чтобы создать файлы кода, или "Проект" > "Добавить существующий элемент", чтобы добавить в проект существующие файлы кода.
//   6. Чтобы снова открыть этот проект позже, выберите пункты меню "Файл" > "Открыть" > "Проект" и выберите SLN-файл.
