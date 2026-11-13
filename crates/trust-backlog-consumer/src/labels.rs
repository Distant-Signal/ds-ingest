//! Per-train service class and operator code (DS-513), written through the
//! sink to `train_service_labels` (ds-core migration 20261113120000), for
//! internal reporting only: the class says whether a TRUST train is a
//! passenger service or a freight, ECS, light-locomotive or engineering
//! working, and `toc_id` is the Activation's operator code. Neither is
//! served by the api or shown anywhere; `toc_id` is never mapped to a name.
//!
//! Like [`crate::reasons`], this runs beside [`crate::process::process_message`]
//! and never changes what the backlog stores. It is off unless
//! `TRAIN_SERVICE_LABELS_ENABLED` is set (the table must exist and the
//! `trust_backlog` role hold its grants first).
//!
//! **The class is a convention, not a fact.** It comes from the first digit
//! of the headcode, the third character of TRUST's 10-character train id
//! (`122A47MG15`: `2` ordinary passenger). The digit says what the train
//! was planned as, and the data bears the ambiguity out:
//!
//! - `6` and `7` are both freight (unfitted or fitted) and engineering
//!   trains, and a headcode digit cannot tell them apart.
//! - `3` is parcels or empty coaching stock, and `5` is empty coaching
//!   stock (the passenger operators' own ECS included).
//! - `9` is special workings: charters, test trains and some regular
//!   passenger services (in a 2026-11 sample nearly all of the class `9`
//!   Activations matched a passenger timetable). It is its own class, and a
//!   reader wanting passenger services must also check for a timetable.
//! - Headcodes are reused across days and operators, so a `1` or `2` can
//!   be a freight working run as a path swap.
//!
//! A train id that is not ten ASCII letters and digits with a digit third
//! is `unknown`, never guessed.

use chrono::NaiveDate;
use trust_schema::schema::TrustMessage;

/// A train's working class. [`ServiceClass::as_str`] is the stored value
/// and must match the table's CHECK list (the migration).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ServiceClass {
    /// Headcode `1`: express passenger.
    PassengerExpress,
    /// Headcode `2`: ordinary passenger.
    PassengerOrdinary,
    /// Headcode `5`: empty coaching stock.
    Ecs,
    /// Headcode `3`: parcels or empty coaching stock.
    EcsOrParcels,
    /// Headcode `4` or `8`: freight.
    Freight,
    /// Headcode `6` or `7`: freight or engineering, undecidable from the
    /// headcode.
    FreightOrEngineering,
    /// Headcode `0`: light locomotive.
    LightLoco,
    /// Headcode `9`: special workings.
    Special,
    /// Anything else, including a malformed train id.
    Unknown,
}

impl ServiceClass {
    /// The stored value.
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::PassengerExpress => "passenger_express",
            Self::PassengerOrdinary => "passenger_ordinary",
            Self::Ecs => "ecs",
            Self::EcsOrParcels => "ecs_or_parcels",
            Self::Freight => "freight",
            Self::FreightOrEngineering => "freight_or_engineering",
            Self::LightLoco => "light_loco",
            Self::Special => "special",
            Self::Unknown => "unknown",
        }
    }
}

/// The class of a TRUST train id, from its headcode's first digit (see the
/// module doc). Only a ten-character ASCII letter-or-digit id with a digit
/// in the third place is read; anything else is [`ServiceClass::Unknown`].
pub(crate) fn service_class_for_train_id(train_id: &str) -> ServiceClass {
    let bytes = train_id.as_bytes();
    if bytes.len() != 10 || !bytes.iter().all(u8::is_ascii_alphanumeric) {
        return ServiceClass::Unknown;
    }
    match bytes[2] {
        b'0' => ServiceClass::LightLoco,
        b'1' => ServiceClass::PassengerExpress,
        b'2' => ServiceClass::PassengerOrdinary,
        b'3' => ServiceClass::EcsOrParcels,
        b'4' | b'8' => ServiceClass::Freight,
        b'5' => ServiceClass::Ecs,
        b'6' | b'7' => ServiceClass::FreightOrEngineering,
        b'9' => ServiceClass::Special,
        _ => ServiceClass::Unknown,
    }
}

/// The Activation's operator code, kept only when it fits the column
/// (one to four ASCII letters or digits). A longer or odd value is dropped
/// rather than risk the batch's insert: the code is a label, not a key.
pub(crate) fn clean_toc_id(raw: Option<&str>) -> Option<String> {
    let toc = raw?.trim();
    (!toc.is_empty() && toc.len() <= 4 && toc.bytes().all(|b| b.is_ascii_alphanumeric()))
        .then(|| toc.to_owned())
}

/// One `train_service_labels` row, before its `trains` row is found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TrainLabel {
    pub train_uid: String,
    pub service_date: NaiveDate,
    pub service_class: ServiceClass,
    pub toc_id: Option<String>,
}

/// The label an Activation makes, or `None` for any other message or an
/// Activation without a `train_uid`. `event` is the backlog event the same
/// message produced: its `service_date` is the one the shared `trains` row
/// is keyed by, so the label is filed under the same row.
pub(crate) fn label_for(
    message: &TrustMessage,
    event: &common::TrustBacklogEventMessage,
) -> Option<TrainLabel> {
    let TrustMessage::Activation(activation) = message else {
        return None;
    };
    if event.msg_type != "0001" {
        return None;
    }
    Some(TrainLabel {
        train_uid: event.train_uid.clone()?,
        service_date: event.service_date,
        service_class: service_class_for_train_id(&activation.train_id),
        toc_id: clean_toc_id(activation.toc_id.as_deref()),
    })
}

#[cfg(test)]
mod tests {
    use trust_schema::schema::Activation;

    use super::*;

    #[test]
    fn the_headcode_digit_is_the_third_character() {
        assert_eq!(
            service_class_for_train_id("122A47MG15"),
            ServiceClass::PassengerOrdinary
        );
        assert_eq!(
            service_class_for_train_id("121A04MG15"),
            ServiceClass::PassengerExpress
        );
        assert_eq!(
            service_class_for_train_id("123Z21XA15"),
            ServiceClass::EcsOrParcels
        );
        assert_eq!(
            service_class_for_train_id("124K60DE13"),
            ServiceClass::Freight
        );
        assert_eq!(service_class_for_train_id("125Y32AB15"), ServiceClass::Ecs);
        assert_eq!(
            service_class_for_train_id("126X20AC15"),
            ServiceClass::FreightOrEngineering
        );
        assert_eq!(
            service_class_for_train_id("127Z30AC15"),
            ServiceClass::FreightOrEngineering
        );
        assert_eq!(
            service_class_for_train_id("128C90MW15"),
            ServiceClass::Freight
        );
        assert_eq!(
            service_class_for_train_id("120Z32XA15"),
            ServiceClass::LightLoco
        );
        assert_eq!(
            service_class_for_train_id("129Q18MG15"),
            ServiceClass::Special
        );
    }

    #[test]
    fn the_location_digits_and_later_characters_do_not_matter() {
        // The first two characters are the origin's location code, not the
        // class: a leading `2` or `9` there changes nothing.
        assert_eq!(service_class_for_train_id("225Y32AB15"), ServiceClass::Ecs);
        assert_eq!(
            service_class_for_train_id("941A04MG15"),
            ServiceClass::PassengerExpress
        );
        assert_eq!(
            service_class_for_train_id("12-A47MG15"),
            ServiceClass::Unknown
        );
    }

    #[test]
    fn a_malformed_train_id_is_unknown() {
        for train_id in [
            "",
            "12",
            "122A47MG1",
            "122A47MG155",
            "12XA47MG15",
            "12 A47MG15",
            "12\u{00e9}A47MG15",
            "12\u{0032}A47MG1\u{00e9}",
        ] {
            assert_eq!(
                service_class_for_train_id(train_id),
                ServiceClass::Unknown,
                "{train_id:?}"
            );
        }
    }

    #[test]
    fn every_stored_value_is_in_the_migrations_check_list() {
        // The migration's CHECK (ds-core 20261113120000).
        let allowed = [
            "passenger_express",
            "passenger_ordinary",
            "ecs",
            "ecs_or_parcels",
            "freight",
            "freight_or_engineering",
            "light_loco",
            "special",
            "unknown",
        ];
        for class in [
            ServiceClass::PassengerExpress,
            ServiceClass::PassengerOrdinary,
            ServiceClass::Ecs,
            ServiceClass::EcsOrParcels,
            ServiceClass::Freight,
            ServiceClass::FreightOrEngineering,
            ServiceClass::LightLoco,
            ServiceClass::Special,
            ServiceClass::Unknown,
        ] {
            assert!(allowed.contains(&class.as_str()), "{class:?}");
        }
    }

    #[test]
    fn the_operator_code_is_kept_only_when_it_fits() {
        assert_eq!(clean_toc_id(Some("55")).as_deref(), Some("55"));
        assert_eq!(clean_toc_id(Some(" 55 ")).as_deref(), Some("55"));
        assert_eq!(clean_toc_id(Some("EWS1")).as_deref(), Some("EWS1"));
        assert_eq!(clean_toc_id(None), None);
        assert_eq!(clean_toc_id(Some("")), None);
        assert_eq!(clean_toc_id(Some("   ")), None);
        assert_eq!(clean_toc_id(Some("12345")), None);
        assert_eq!(clean_toc_id(Some("5 5")), None);
        assert_eq!(clean_toc_id(Some("5\u{00e9}")), None);
    }

    fn activation(train_id: &str, toc_id: Option<&str>) -> TrustMessage {
        TrustMessage::Activation(Activation {
            train_id: train_id.to_owned(),
            train_uid: "C12345".to_owned(),
            toc_id: toc_id.map(str::to_owned),
            train_service_code: None,
            schedule_wtt_id: None,
            schedule_start_date: None,
            schedule_end_date: None,
            tp_origin_timestamp: None,
        })
    }

    fn event(msg_type: &str, train_uid: Option<&str>) -> common::TrustBacklogEventMessage {
        common::TrustBacklogEventMessage {
            crs: None,
            train_uid: train_uid.map(str::to_owned),
            train_id: "126X20AC15".to_owned(),
            service_date: "2026-11-13".parse().unwrap(),
            msg_type: msg_type.to_owned(),
            event_type: None,
            planned_timestamp: None,
            actual_timestamp: None,
            variation_status: None,
            delay_minutes: None,
            dedup_key: "k".to_owned(),
            gbtt_timestamp: None,
            platform: None,
        }
    }

    #[test]
    fn an_activation_makes_a_label_filed_under_its_events_date() {
        let label = label_for(
            &activation("126X20AC15", Some("55")),
            &event("0001", Some("C12345")),
        )
        .unwrap();
        assert_eq!(label.train_uid, "C12345");
        assert_eq!(label.service_date.to_string(), "2026-11-13");
        assert_eq!(label.service_class, ServiceClass::FreightOrEngineering);
        assert_eq!(label.toc_id.as_deref(), Some("55"));
    }

    #[test]
    fn only_an_activation_with_a_uid_makes_a_label() {
        assert_eq!(
            label_for(&activation("126X20AC15", None), &event("0001", None)),
            None
        );
        assert_eq!(
            label_for(
                &activation("126X20AC15", None),
                &event("0003", Some("C12345"))
            ),
            None
        );
        let movement = TrustMessage::Unknown("0008".to_owned());
        assert_eq!(label_for(&movement, &event("0001", Some("C12345"))), None);
    }
}
