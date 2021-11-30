using MicroRabbit.Transfer.Application.Interfaces;
using MicroRabbit.Transfer.Domain.Models;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace MicroRobbit.Transfer.Api.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class TransforController : ControllerBase
    {
        private readonly ITransferService _transferService;

        public TransforController(ITransferService transferService)
        {
            _transferService = transferService;
        }
        [HttpGet]
        public ActionResult<IEnumerable<TransferLog>> Get()
        {
            return Ok(_transferService.GetTransferLogs());
        }
    }
}
