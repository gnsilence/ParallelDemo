using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace Parallel.Demo
{
    class Program
    {
        static CSRedis.CSRedisClient csredis = new CSRedis.CSRedisClient("127.0.0.1:6379,password=abc@123");
        static int Batchsize = 1000;//接收多少条处理一次
        static DateTime lastsubtime;
        static System.Timers.Timer timer = new System.Timers.Timer(2000);
        static List<Person> persons = new List<Person>();
        private static ManualResetEvent _exit = null;
        //定义批处理接收体,Greedy(true,贪婪模式，给多少要多少，false的话必须给够足够数量才接收)
        static BatchBlock<Person> batchpersons = new BatchBlock<Person>(Batchsize, new GroupingDataflowBlockOptions() { Greedy = true });
        static void Main(string[] args)
        {
            timer.Start();
            _exit = new ManualResetEvent(false);
            Console.WriteLine("程序已经启动，输入Ctr+Break可以退出程序");
            //接收多少条数据进行处理
            AddPersonBatched();
        }

        static void AddPersonBatched()
        {
            //监视
            timer.Elapsed += Timer_Elapsed;

            //定义接收后执行的操作
            var insertpersons = new ActionBlock<Person[]>(p => InsertPersons(p));

            //连接数据源
            batchpersons.LinkTo(insertpersons);

            //post数据的入口1
            PostData(batchpersons);

            //post数据的入口2
            PostDataByAnotherChanel(batchpersons);

            batchpersons.Completion.ContinueWith(delegate { insertpersons.Complete(); });

            batchpersons.Complete();

            insertpersons.Completion.Wait();
        }
        /// <summary>
        /// 监视，过多久接收不到数据，就把已经接收的发给执行目标
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private static void Timer_Elapsed(object sender, System.Timers.ElapsedEventArgs e)
        {
            if ((DateTime.Now - lastsubtime).TotalMinutes >5)
            {
                batchpersons.TriggerBatch();
            }
        }
        /// <summary>
        /// 订阅频道1
        /// </summary>
        /// <param name="target"></param>
        /// <returns></returns>
        static Task PostData(ITargetBlock<Person> target)
        {

            var taskreslut = Task.Run(() =>
            {
                csredis.Subscribe(("msg", msg =>
                {
                    Person p = new Person();
                    p.Name = msg.Body;
                    target.Post(p);
                    lastsubtime = DateTime.Now;
                    Console.WriteLine(msg.Body);
                }
                ));
            });
            return Task.CompletedTask;
        }

        /// <summary>
        /// 订阅频道2
        /// </summary>
        /// <param name="target"></param>
        /// <returns></returns>
        static Task PostDataByAnotherChanel(ITargetBlock<Person> target)
        {
            var taskreslut = Task.Run(() =>
            {

                csredis.Subscribe(("bsd", msg =>
                {
                    Person p = new Person();
                    p.Name = msg.Body;
                    target.Post(p);
                    lastsubtime = DateTime.Now;
                    Console.WriteLine(msg.Body);
                }
                ));
            });
            //阻塞当前线程，只在最后一个频道处阻塞
            Console.CancelKeyPress += (sender, e) =>
            {
                if (e.SpecialKey == ConsoleSpecialKey.ControlBreak)
                {
                    Console.WriteLine("退出");
                    return;
                }

                e.Cancel = true;

                _exit.Set();
            };
            _exit?.WaitOne();
            return Task.CompletedTask;
        }

        static void InsertPersons(Person[] peoples)
        {
            //多线程，会发生争抢，因此需要开个线程去输出结果
            new TaskFactory().StartNew(() =>
            {
                Console.WriteLine($"接收到{peoples.Length}条数据，随机测试数据{peoples[1].Name}");
                persons.AddRange(peoples);
                Console.WriteLine($"总数据条数{persons.Count}");
            });
        }
    }

    public class Person
    {
        public string Name { get; set; }
    }
}
