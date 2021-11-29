using MediatR;
using MicroRabbit.Banking.Domain.Commands;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace MicroRabbit.Banking.Domain.CommandHandlers
{
    public class TransferCommandHandler : IRequestHandler<CreateTransferCommand, bool>
    {
        public Task<bool> Handle(CreateTransferCommand request, CancellationToken cancellationToken)
        {
            //publish event to RabbitMQ
            return Task.FromResult(true);
        }
    }
}
