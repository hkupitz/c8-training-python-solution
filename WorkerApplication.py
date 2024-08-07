import asyncio

from pyzeebe import ZeebeClient, ZeebeWorker, create_camunda_cloud_channel, Job

grpc_channel = create_camunda_cloud_channel(client_id="V7ItVeTtyA9_2gE53YC0yQoX2FVMp2cR",
                                            client_secret="VeIO_~Wj~dgyucFJAKVi8pE7dso_Gx3VtQ.Rh11ZMk957jvsgJpf9zJ2zK4vxOPJ",
                                            cluster_id="e39f4a7d-5d6b-447c-9878-c27048b6043f",
                                            region="bru-2")
zeebe_client = ZeebeClient(grpc_channel)
worker = ZeebeWorker(grpc_channel)

def get_customer_credit(customer_id: str):
    return customer_id[-2:]

def deduct_credit(customer_id: str, amount: float):
    open_amount = amount - float(get_customer_credit(customer_id))
    return open_amount

@worker.task("credit-deduction")
def handle_credit_deduction(job: Job, customerId: str, orderTotal: float):
    print("Handling job: " + job.type)
    open_amount = deduct_credit(customerId, orderTotal)
    customer_credit = get_customer_credit(customerId)
    return {'openAmount': open_amount, 'customerCredit': customer_credit}

@worker.task("credit-card-charging")
def handle_credit_card_charging(job: Job, cardNumber: str, cvc: str, expiryDate: str):
    print("Handling job: " + job.type)
    print("Charging credit card with number " + cardNumber + ", cvc " + cvc + ", expiry date " + expiryDate)
    return


asyncio.get_event_loop().run_until_complete(worker.work())



