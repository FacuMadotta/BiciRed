pub enum MessageType {
    RentRequest,
    ReturnRequest,
    RentConfirmed,
    RentRejected,
    ReturnConfirmed,
    ReturnRejected,
    StationUpdate,
    IsAlive,
    Election,
    Coordinator,
    Ack,
    NearbyResponse,
    PreparePayment,
    VoteCommit,
    VoteAbort,
    CommitPayment,
    CapturePayment,
    RollbackPayment,
}

impl MessageType {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::RentRequest => "RENT_REQUEST",
            Self::ReturnRequest => "RETURN_REQUEST",
            Self::RentConfirmed => "RENT_CONFIRMED",
            Self::RentRejected => "RENT_REJECTED",
            Self::ReturnConfirmed => "RETURN_CONFIRMED",
            Self::ReturnRejected => "RETURN_REJECTED",
            Self::StationUpdate => "STATION_UPDATE",
            Self::IsAlive => "IS_ALIVE",
            Self::Election => "ELECTION",
            Self::Coordinator => "COORDINATOR",
            Self::Ack => "ACK",
            Self::NearbyResponse => "NEARBY_RESPONSE",
            Self::PreparePayment => "PREPARE_PAYMENT",
            Self::VoteCommit => "VOTE_COMMIT",
            Self::VoteAbort => "VOTE_ABORT",
            Self::CommitPayment => "COMMIT_PAYMENT",
            Self::CapturePayment => "CAPTURE_PAYMENT",
            Self::RollbackPayment => "ROLLBACK_PAYMENT",
        }
    }

    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "RENT_REQUEST" => Some(Self::RentRequest),
            "RETURN_REQUEST" => Some(Self::ReturnRequest),
            "RENT_CONFIRMED" => Some(Self::RentConfirmed),
            "RENT_REJECTED" => Some(Self::RentRejected),
            "RETURN_CONFIRMED" => Some(Self::ReturnConfirmed),
            "RETURN_REJECTED" => Some(Self::ReturnRejected),
            "STATION_UPDATE" => Some(Self::StationUpdate),
            "IS_ALIVE" => Some(Self::IsAlive),
            "ELECTION" => Some(Self::Election),
            "COORDINATOR" => Some(Self::Coordinator),
            "ACK" => Some(Self::Ack),
            "NEARBY_RESPONSE" => Some(Self::NearbyResponse),
            "PREPARE_PAYMENT" => Some(Self::PreparePayment  ),
            "VOTE_COMMIT" => Some(Self::VoteCommit),
            "VOTE_ABORT" => Some(Self::VoteAbort),
            "COMMIT_PAYMENT" => Some(Self::CommitPayment),
            "CAPTURE_PAYMENT" => Some(Self::CapturePayment),
            "ROLLBACK_PAYMENT" => Some(Self::RollbackPayment),            
            _ => None,
        }
    }
}
