#[pyfunction]
fn prepare_address_batch(py: Python<'_>, rows: Vec<AddressRow>) -> PyResult<Py<PyList>> {
    let pool = location_canon_pool()?;
    let prepared = py.detach(|| {
        pool.install(|| {
            rows.par_iter()
                .map(|row| {
                    crate::address_canon::display::prepare_address([
                        row.0.as_deref(),
                        row.1.as_deref(),
                        row.2.as_deref(),
                        row.3.as_deref(),
                        row.4.as_deref(),
                        row.5.as_deref(),
                    ])
                })
                .collect::<Vec<_>>()
        })
    });
    let result = PyList::empty(py);
    for item in prepared {
        if let Some(item) = item {
            let fields = canonical_to_dict(py, &item.canonical)?;
            fields.set_item("formatted_address", item.formatted_address.as_deref())?;
            fields.set_item("entity_street_key", &item.entity.street_key)?;
            fields.set_item("entity_line1_norm", item.entity.line1_norm.as_deref())?;
            fields.set_item("entity_line2_norm", item.entity.line2_norm.as_deref())?;
            fields.set_item("entity_city_norm", item.entity.city_norm.as_deref())?;
            fields.set_item("entity_state_code", item.entity.state_code.as_deref())?;
            fields.set_item("entity_zip5", item.entity.zip5.as_deref())?;
            fields.set_item("entity_country_norm", item.entity.country_norm.as_deref())?;
            fields.set_item(
                "entity_keyable",
                if item.entity.keyable { "true" } else { "false" },
            )?;
            result.append(fields)?;
        } else {
            result.append(py.None())?;
        }
    }
    Ok(result.into())
}

#[cfg(test)]
mod address_batch_tests {
    use super::*;

    #[test]
    fn address_batch_preserves_null_fallback_and_canonical_fields() {
        Python::initialize();
        Python::attach(|py| {
            let row = (
                Some("100 Main St".to_owned()),
                None,
                Some("CITY".to_owned()),
                Some("MO".to_owned()),
                Some("64055".to_owned()),
                Some("US".to_owned()),
            );
            let unicode = (Some("Caf\u{e9}".to_owned()), None, None, None, None, None);
            let prepared = prepare_address_batch(
                py,
                vec![row.clone(), unicode, (None, None, None, None, None, None)],
            )
            .unwrap();
            let prepared = prepared.bind(py);
            assert_eq!(prepared.len(), 3);
            let address = prepared.get_item(0).unwrap();
            assert_eq!(
                address
                    .get_item("formatted_address")
                    .unwrap()
                    .extract::<String>()
                    .unwrap(),
                "100 Main Street, City, MO 64055"
            );
            let original = canonicalize_batch(py, vec![row]).unwrap();
            let original = original.bind(py).get_item(0).unwrap();
            for key in [
                "address_key",
                "identity_key",
                "premise_key",
                "premise_identity_key",
                "line1_norm",
                "unit_norm",
                "city_norm",
                "state_code",
                "zip5",
                "zip4",
                "country_code",
            ] {
                assert!(address
                    .get_item(key)
                    .unwrap()
                    .eq(original.get_item(key).unwrap())
                    .unwrap());
            }
            for (key, expected) in [
                ("entity_street_key", Some("100mainst")),
                ("entity_line1_norm", Some("100mainst")),
                ("entity_line2_norm", None),
                ("entity_city_norm", Some("city")),
                ("entity_state_code", Some("MO")),
                ("entity_zip5", Some("64055")),
                ("entity_country_norm", Some("US")),
                ("entity_keyable", Some("true")),
            ] {
                assert_eq!(
                    address
                        .get_item(key)
                        .unwrap()
                        .extract::<Option<String>>()
                        .unwrap()
                        .as_deref(),
                    expected,
                    "{key}"
                );
            }
            assert!(prepared.get_item(1).unwrap().is_none());
            assert!(prepared
                .get_item(2)
                .unwrap()
                .get_item("formatted_address")
                .unwrap()
                .is_none());
            let empty = prepared.get_item(2).unwrap();
            assert_eq!(
                empty
                    .get_item("entity_street_key")
                    .unwrap()
                    .extract::<String>()
                    .unwrap(),
                ""
            );
            assert!(empty.get_item("entity_country_norm").unwrap().is_none());
            assert_eq!(
                empty
                    .get_item("entity_keyable")
                    .unwrap()
                    .extract::<String>()
                    .unwrap(),
                "false"
            );
            assert_eq!(prepare_address_batch(py, vec![]).unwrap().bind(py).len(), 0);
            let module = PyModule::new(py, "ptg2_address_canon").unwrap();
            ptg2_address_canon(&module).unwrap();
            assert!(module.hasattr("prepare_address_batch").unwrap());
        });
    }
}
