import asyncio

from pyzeebe import ZeebeClient, ZeebeWorker, create_camunda_cloud_channel, Job

grpc_channel = create_camunda_cloud_channel(client_id="",
                                            client_secret="",
                                            cluster_id="",
                                            region="")
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



