package kafkastream.pojos

data class DeliveryAddress(
    val addressLine: AddressLine = AddressLine(),
    val city: City = City(),
    val contactNumber: ContactNumber = ContactNumber(),
    val pinCode: PinCode = PinCode(),
    val state: State = State()
) {
    data class AddressLine(
        val type: String = ""
    )

    data class City(
        val type: String = ""
    )

    data class ContactNumber(
        val type: String = ""
    )

    data class PinCode(
        val type: String = ""
    )

    data class State(
        val type: String = ""
    )
}
