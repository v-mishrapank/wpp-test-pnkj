locals {


  ucp_member_entra_ids = [
    {
      email_address = "v-Owen.Lundberg@wpp.com",
      object_id     = "75d958d9-57f2-4f32-a99f-16625322f96c"
    },
    {
      email_address = "v-maheshpanda@wpp.com",
      object_id     = "0524c4c4-21e4-42aa-bd30-fef516fd80f9"
    },
    {
      email_address = "v-ratkumar@wpp.com",
      object_id     = "cca15b67-0e90-4ca6-8c90-2cf2a9b44378"
    },
    {
      email_address = "v-sumitarora@wpp.com",
      object_id     = "541d398a-185d-43a6-b860-3789f57bf9be"
    },
    {
      email_address = "v-mmunendra@wpp.com",
      object_id     = "b88cd19e-c83f-4017-8ad5-f37c8fd8393c"
    }
  ]

  ucp_spn_assignments = [
    {
      subscription_name           = "sub-wpp-wppet-ucp-example-d-001"
      service_principal_name      = "spn-sub-wpp-wppet-ucp-example-d-001"
      service_principal_object_id = "86f16731-aebb-4369-9654-701accd820ee"
      role                        = "Contributor"
    },
  ]

}
