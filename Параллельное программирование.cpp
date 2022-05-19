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
#include <filesystem>

using namespace std;
namespace fs = filesystem;

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
void thread_work(SafeQueue<string>& q, int my_num)
{
	while (true)
	{
		string a, b;
		fs::path cur_p = fs::current_path();
		bool go = q.wait_pop(a, b);
		if (!go)
			break;
		q.set_me_working(my_num, true);
		string new_file_name = a + b;
		ofstream out(new_file_name, ios::binary);
		ifstream in1(a, ios::binary);
		ifstream in2(b, ios::binary);
		int num1, num2;
		in1.read((char*)&num1, sizeof(int));
		in2.read((char*)&num2, sizeof(int));
		while (in1.peek() != EOF && in2.peek() != EOF) {
			if (num1 < num2) {
				out.write((char*)&num1, sizeof(num1));
				in1.read((char*)&num1, sizeof(int));
			}
			else {
				out.write((char*)&num2, sizeof(num2));
				in1.read((char*)&num2, sizeof(int));
			}
		}
		if (in1.peek() == EOF) {
			while (in2.peek() != EOF) {
				out.write((char*)&num2, sizeof(num2));
				in1.read((char*)&num2, sizeof(int));
			}
		}
		else {
			out.write((char*)&num1, sizeof(num1));
			in1.read((char*)&num1, sizeof(int));
		}
		in1.close();
		in2.close();
		remove(cur_p / a);
		remove(cur_p / b);
		out.close();
		this_thread::sleep_for(chrono::milliseconds(100));
		q.push(new_file_name);
		q.set_me_working(my_num, false);
	}
	q.set_me_working(my_num, false);
}


string merged_sorted_file(SafeQueue<string>& q, int req_num_treads)
{
	int max_threads = thread::hardware_concurrency();
	int num_threads = min(max_threads, req_num_treads);
	vector<thread> threads(num_threads - 1);

	try
	{
		int tres, tstart, tend;
		auto start = std::chrono::steady_clock::now();

		for (int i = 0; i < num_threads - 1; i++)
			threads[i] = thread(thread_work, ref(q), i);
		thread_work(ref(q), num_threads - 1);

		for_each(threads.begin(), threads.end(), mem_fn(&thread::join));

		auto end = std::chrono::steady_clock::now();
		auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

		cout << "Done by " << num_threads << " thread(s) in " << elapsed.count() << " milisecs." << endl;

		return q.just_pop();

	}
	catch (...)
	{
		for_each(threads.begin(), threads.end(), mem_fn(&thread::join));
		throw;
	}

}

int read_from(ifstream& in, int N, vector<int>& V, int pos, SafeQueue<string>& q) {
	int iter = 0;
	cout << "Id потока " << this_thread::get_id() << endl;
	try {
		string filename = to_string(pos);
		ofstream out(filename, ios::binary);
		q.push(filename);
		while (in.peek() != EOF && iter < N) {
			int i;
			in.read((char*)&i, sizeof(int));
			iter++;
			V.push_back(i);
		}
		sort(V.begin(), V.end());
		for (int i = 0; i < V.size(); i++) {
			out.write((char*)&V[i], sizeof(V[i]));
		}
		out.close();
	}
	catch (...) {
		cout << "Oops" << endl;
	}
	return iter;
}
void f(mutex& m, ifstream& in, int pos,int &count, SafeQueue<string>& q) {
	lock_guard<mutex> guard(m);
	vector<int> V;
	int cnt = read_from(in, count, V, pos, q);
	cout << "Количество прочитанных файлов " << cnt << endl;
	for (int i = 0; i < V.size(); i++) {
		cout << V[i] << " " << endl;
	}
}

long int count_of_int(ifstream& in) {
	in.seekg(0, ios::end);
	long int n = in.tellg();
	in.seekg(0);
	return n / sizeof(int);
}

int main()
{
	//Creating files to test work of the programm

	ofstream out("test1.dat", ios::binary);
	int N = 100;
	for (int i = 1; i < N + 1; i++) {
		out.write((char*)&i, sizeof(i));
	}
	out.close();

	//Begin of the programm

	ifstream in("test1.dat", ios::binary);
	mutex m;
	queue<string> default_queue;
	int max_threads = thread::hardware_concurrency();
	cout << "Максимальное количество потоков " << max_threads << endl;
	vector<thread> threads(max_threads);
	SafeQueue<string> q(default_queue, max_threads);
	long int intsinfile = count_of_int(in);
	int cnt = (int)intsinfile / max_threads;
	for (int i = 0; i < max_threads-1; i++)
		threads[i] = thread(f, ref(m), ref(in), i * cnt,ref(cnt), ref(q));
	cnt = (int)intsinfile - (max_threads - 1) * cnt;
	threads[max_threads-1] = thread(f, ref(m), ref(in),  (max_threads-1)* cnt, ref(cnt), ref(q));
	for_each(threads.begin(), threads.end(), mem_fn(&thread::join));
	in.close();
	string file_name = merged_sorted_file(q, max_threads);
	cout << file_name << endl;
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
