using MediatR;
using MicroRabbit.Domain.Core.Bus;
using MicroRabbit.Domain.Core.Commands;
using MicroRabbit.Domain.Core.Events;
using Newtonsoft.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MicroRabbit.Infra.Bus
{
    public sealed class RabbitMQBus : IEventBus
    {
        private readonly IMediator _mediator;
        private readonly Dictionary<string, List<Type>> _handlers;
        private readonly List<Type> _eventType;

        public RabbitMQBus(IMediator mediator)
        {
            _mediator = mediator;
            _handlers = new Dictionary<string, List<Type>>();
            _eventType = new List<Type>();
        }
        public Task SendCommand<T>(T command) where T : Command
        {
            return _mediator.Send(command);
        }

        public void Publish<T>(T @event) where T : Event
        {
            var factory = new ConnectionFactory() { HostName = "localhost" };
            using (var cannection = factory.CreateConnection())
            using (var channel = cannection.CreateModel())
            {
                var eventName = @event.GetType().Name;
                channel.QueueDeclare(eventName, false, false, false, null);

                var message = JsonConvert.SerializeObject(@event);
                var body = Encoding.UTF8.GetBytes(message);

                channel.BasicPublish("", eventName, null, body);
            }
        }


        public void Subscribe<T, TH>()
            where T : Event
            where TH : IEventHandler<T>
        {
            var eventName = typeof(T).GetType().Name;
            var handlerType = typeof(TH);

            if (!_eventType.Contains(typeof(T)))
            {
                _eventType.Add(typeof(T));
            }
            if (!_handlers.ContainsKey(eventName))
            {
                _handlers.Add(eventName, new List<Type>());
            }
            if (_handlers[eventName].Any(s => s.GetType() == handlerType))
            {
                throw new ArgumentException($"Handler Type {handlerType.Name} already is registered for {eventName}", nameof(handlerType));
            }
            _handlers[eventName].Add(handlerType);

            StartBasicConsume<T>();
        }

        private void StartBasicConsume<T>() where T : Event
        {
            var factory = new ConnectionFactory()
            {
                HostName = "localhost",
                DispatchConsumersAsync = true
            };
            using (var cannection = factory.CreateConnection())
            using (var channel = cannection.CreateModel())
            {
                var eventName = typeof(T).Name;

                channel.QueueDeclare(eventName, false, false, false, null);
                var consumer = new EventingBasicConsumer(channel);


                consumer.Received  += Consumer_Received; ;
                channel.BasicConsume(eventName, true, consumer);
            }
        }


        private void Consumer_Received(object sender, BasicDeliverEventArgs e)
        {
            var eventName = e.RoutingKey;
            var massage = Encoding.UTF8.GetString(e.Body.ToArray());
            try
            {
                ProcessEvent(eventName, massage).ConfigureAwait(false);

            }
            catch (Exception ex)
            {

            }
        }

        private async Task ProcessEvent(string eventName, string massage)
        {
            if(_handlers.ContainsKey(eventName))
            {
                var subscriptins = _handlers[eventName];
                foreach (var subscription in subscriptins)
                {
                    var handler = Activator.CreateInstance(subscription);
                    if (handler == null) continue;
                    var eventType = _eventType.SingleOrDefault(t => t.Name == eventName);
                    var @event = JsonConvert.DeserializeObject(massage, eventType);
                    var concriteType = typeof(IEventHandler<>).MakeGenericType(eventType);
                    await (Task)concriteType.GetMethod("Handle").Invoke(handler, new object[] { @event });
                }
            }
        }
    }
}
